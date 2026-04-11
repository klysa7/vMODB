package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.modb.rel.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

/**
 * B38 FIX: Thread-safe DistributedPlanner.
 *
 * BEFORE: subplansAccumulator and exchangeCounter were instance fields:
 *
 *   private List<Object> subplansAccumulator;
 *   private int exchangeCounter;
 *
 *   public DistributedPlan create(RelNode physicalPlan, Long snapshot) {
 *       this.subplansAccumulator = new ArrayList<>();  // shared write
 *       this.exchangeCounter = 0;                      // shared write
 *       ...
 *   }
 *
 *   DistributedPlanner is a single shared instance across all HTTP threads
 *   (built once in OlapGatewayService.buildOrchestrator() and cached).
 *   With α=2, two threads call planner.create() simultaneously:
 *     Thread 1: this.subplansAccumulator = new ArrayList<>()
 *     Thread 2: this.subplansAccumulator = new ArrayList<>()  ← wipes Thread 1's reference
 *     Thread 1: subplansAccumulator.add(scanSubplan)          ← adds to wrong list
 *     Result: corrupted or incomplete distributed plan.
 *
 *   In practice, QPO-1 mitigates this — planner.create() is only called on the
 *   first query per SQL string, before α=2 concurrent calls are likely to race.
 *   But the race is theoretically possible during warmup with α=2.
 *
 * AFTER: subplansAccumulator and exchangeCounter are LOCAL variables inside create().
 *   Each thread gets its own list and counter on its own stack frame.
 *   No shared mutable state — create() is now fully thread-safe.
 *   No synchronization needed, no performance cost.
 *
 * Cite: Bernstein & Goodman 1981 "Concurrency Control in Distributed Database
 *   Systems" — eliminating shared mutable state is preferable to synchronizing it.
 *   Java Memory Model (JLS §17.4) — method-local variables are always thread-safe.
 *
 * Thread safety summary after B38:
 *   DistributedPlanner.create()    — thread-safe (local state only)
 *   DistributedExecutor.execute()  — thread-safe (ConcurrentHashMap QPO-7, local ops)
 *   Orchestrator.execute()         — thread-safe (delegates to above, no instance writes)
 *   Verified by α=2 logs: queryId 9 and 10 execute concurrently without serialization.
 */
public final class DistributedPlanner {

    private static final System.Logger LOGGER =
            System.getLogger(Orchestrator.class.getName());

    private final CoordinatorCatalog catalog;
    private final ColumnsResolver columnsResolver;

    // B38 FIX: subplansAccumulator and exchangeCounter removed as instance fields.
    // They are now local variables in create() — each call gets its own stack frame.
    // BEFORE: private List<Object> subplansAccumulator;
    // BEFORE: private int exchangeCounter;

    public DistributedPlanner(CoordinatorCatalog catalog, ColumnsResolver columnsResolver) {
        this.catalog = catalog;
        this.columnsResolver = columnsResolver;
    }

    public DistributedPlan create(RelNode physicalPlan, Long snapshot) {
        // B38 FIX: local variables — thread-safe, each call has its own copy.
        // BEFORE: this.subplansAccumulator = new ArrayList<>();
        // BEFORE: this.exchangeCounter = 0;
        List<Object> subplansAccumulator = new ArrayList<>();
        int[] exchangeCounter = {0}; // array wrapper to allow mutation inside lambda

        CoordinatorOperatorDefinition rootOperation =
                relNodeToOperatorTree(physicalPlan, subplansAccumulator, exchangeCounter);

        LOGGER.log(INFO, "I entered create DistributedPlanner " + rootOperation);
        return new DistributedPlan(snapshot, new ArrayList<>(subplansAccumulator), rootOperation);
    }

    private CoordinatorOperatorDefinition relNodeToOperatorTree(
            RelNode node,
            List<Object> subplansAccumulator,
            int[] exchangeCounter) {

        if (node instanceof VModbProject project) {
            return new ProjectDefinition(
                    relNodeToOperatorTree(project.getInput(), subplansAccumulator, exchangeCounter),
                    project.getProjects());
        }

        if (node instanceof VModbJoin join) {
            return new JoinDefinition(
                    relNodeToOperatorTree(join.getLeft(),  subplansAccumulator, exchangeCounter),
                    relNodeToOperatorTree(join.getRight(), subplansAccumulator, exchangeCounter),
                    join.leftJoinCols, join.rightJoinCols);
        }

        if (node instanceof VModbFilter filter) {
            if (filter.getInput() instanceof VModbTableAccess scan) {
                return createScanSubplan(scan, filter.getCondition(),
                        subplansAccumulator, exchangeCounter);
            }
        }

        if (node instanceof VModbAggregate agg) {
            CoordinatorOperatorDefinition inputOp =
                    relNodeToOperatorTree(agg.getInput(), subplansAccumulator, exchangeCounter);
            int[] groupByIndices = agg.getGroupSet().toArray();
            List<AggregateDefinition.AggCallDef> aggCalls = new ArrayList<>();
            for (AggregateCall call : agg.getAggCallList()) {
                String kind = call.getAggregation().getKind() == SqlKind.COUNT ? "COUNT"
                        : call.getAggregation().getKind() == SqlKind.SUM    ? "SUM"
                        : call.getAggregation().getKind() == SqlKind.AVG    ? "AVG"
                        : call.getAggregation().getKind() == SqlKind.MIN    ? "MIN"
                        : call.getAggregation().getKind() == SqlKind.MAX    ? "MAX"
                        : call.getAggregation().getName();
                int argIndex = call.getArgList().isEmpty() ? -1 : call.getArgList().get(0);
                aggCalls.add(new AggregateDefinition.AggCallDef(kind, argIndex));
            }
            return new AggregateDefinition(inputOp, groupByIndices, aggCalls);
        }

        if (node instanceof VModbTableAccess scan) {
            return createScanSubplan(scan, null, subplansAccumulator, exchangeCounter);
        }

        if (node.getInputs().size() == 1) {
            return relNodeToOperatorTree(node.getInput(0), subplansAccumulator, exchangeCounter);
        }

        throw new IllegalArgumentException(
                "Unsupported Operator: " + node.getClass().getSimpleName());
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    public record ColRefDTO(int columnPosition) {}
    public record PredicateDTO(ColRefDTO columnReference, String expression, Object value) {}

    private ScanDefinition createScanSubplan(VModbTableAccess scan, RexNode condition,
                                             List<Object> subplansAccumulator,
                                             int[] exchangeCounter) {
        String schema     = scan.getSchemaName();
        String table      = scan.getTableName();
        // B38 FIX: use local exchangeCounter[0] instead of this.exchangeCounter
        String exchangeId = "exchange_" + (exchangeCounter[0]++);

        List<String> columns     = columnsResolver.columnsInOrder(schema, table);
        byte[]       predicates  = extractPredicates(condition);
        String       vmsAddr     = catalog.getVmsAddress(schema, table);
        String       endpointUrl = "http://" + vmsAddr + "/" + table;

        ScanSubPlan subplan = new ScanSubPlan(
                schema, endpointUrl, exchangeId,
                new ScanAllOperation(schema, table),
                columns, predicates,
                null   // columnDescriptors resolved later in DistributedExecutor
        );
        // B38 FIX: use local subplansAccumulator instead of this.subplansAccumulator
        subplansAccumulator.add(subplan);

        return new ScanDefinition(exchangeId, columns, predicates);
    }

    private byte[] extractPredicates(RexNode filter) {
        if (filter == null) return new byte[0];
        List<PredicateDTO> dtos = new ArrayList<>();
        if (filter.getKind() == SqlKind.AND) {
            for (RexNode operand : ((RexCall) filter).getOperands()) {
                PredicateDTO p = parseSingleCondition(operand);
                if (p != null) dtos.add(p);
            }
        } else {
            PredicateDTO p = parseSingleCondition(filter);
            if (p != null) dtos.add(p);
        }
        if (dtos.isEmpty()) return new byte[0];
        try {
            return MAPPER.writeValueAsBytes(dtos);
        } catch (Exception e) {
            LOGGER.log(INFO, "Failed to serialize predicates, falling back to full scan.");
            return new byte[0];
        }
    }

    private PredicateDTO parseSingleCondition(RexNode node) {
        if (!(node instanceof RexCall call)) return null;
        if (call.getOperands().size() != 2) return null;
        if (!(call.getOperands().get(0) instanceof RexInputRef columnRef)) return null;
        if (!(call.getOperands().get(1) instanceof org.apache.calcite.rex.RexLiteral literal))
            return null;

        String exprType = switch (call.getKind()) {
            case EQUALS                -> "EQUALS";
            case GREATER_THAN          -> "GREATER_THAN";
            case LESS_THAN             -> "LESS_THAN";
            case GREATER_THAN_OR_EQUAL -> "GREATER_THAN_OR_EQUAL";
            case LESS_THAN_OR_EQUAL    -> "LESS_THAN_OR_EQUAL";
            case NOT_EQUALS            -> "NOT_EQUALS";
            default                    -> null;
        };
        if (exprType == null) return null;
        return new PredicateDTO(
                new ColRefDTO(columnRef.getIndex()), exprType, literal.getValue3());
    }

    public interface ColumnsResolver {
        List<String> columnsInOrder(String schema, String table);
        List<CatalogColumn> columnMetas(String schema, String table);
    }
}