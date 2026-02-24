package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbFilter;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbJoin;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbProject;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public final class DistributedPlanner {

    private static final System.Logger LOGGER = System.getLogger(Orchestrator.class.getName());

    private final PlacementResolver placement;
    private final ColumnsResolver columnsResolver;
    private List<VmsSubplan> subplansAccumulator;
    private int exchangeCounter;


    public DistributedPlanner(PlacementResolver placement, ColumnsResolver columnsResolver) {
        this.placement = placement;
        this.columnsResolver = columnsResolver;
    }

    public DistributedPlan create(RelNode physicalPlan, Long snapshot) {
        this.subplansAccumulator = new ArrayList<>();
        this.exchangeCounter = 0;

        CoordinatorOperatorDefinition rootOperation = relNodeToOperatorTree(physicalPlan);

        LOGGER.log(INFO,"I entered create DistributedPlanner " + rootOperation);

        return new DistributedPlan(snapshot, new ArrayList<>(subplansAccumulator), rootOperation);
    }

    private CoordinatorOperatorDefinition relNodeToOperatorTree(RelNode node) {

        if (node instanceof VModbProject project) {
            CoordinatorOperatorDefinition inputOperation = relNodeToOperatorTree(project.getInput());
            return new ProjectDefinition(inputOperation, project.getProjects());
        }

        if (node instanceof VModbJoin join) {
            CoordinatorOperatorDefinition leftOperation = relNodeToOperatorTree(join.getLeft());
            CoordinatorOperatorDefinition rightOperation = relNodeToOperatorTree(join.getRight());
            return new JoinDefinition(leftOperation, rightOperation, join.leftJoinCols, join.rightJoinCols);
        }

        if (node instanceof VModbFilter filter) {
            if (filter.getInput() instanceof VModbTableAccess scan) {
                return createScanSubplan(scan, filter.getCondition());
            }
        }

        if (node instanceof VModbTableAccess scan) {
            return createScanSubplan(scan, null);
        }

        if (node.getInputs().size() == 1) {
            return relNodeToOperatorTree(node.getInput(0));
        }

        throw new IllegalArgumentException("Unsupported Operator: " + node.getClass().getSimpleName());
    }

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = new com.fasterxml.jackson.databind.ObjectMapper();

    public record ColRefDTO(int columnPosition) {}
    public record PredicateDTO(ColRefDTO columnReference, String expression, Object value) {}

    private ScanDefinition createScanSubplan(VModbTableAccess scan, RexNode condition) {
        String schema = scan.getSchemaName();
        String table = scan.getTableName();
        String exchangeId = "exchange_" + (exchangeCounter++);

        List<String> columns = columnsResolver.columnsInOrder(schema, table);

        byte[] predicatesJson = extractPredicates(condition);

        VmsSubplan subplan = new VmsSubplan(
                placement.ownerVms(schema, table),
                placement.endpointUrl(schema, table),
                exchangeId,
                new ScanAllOperation(schema, table),
                columns,
                predicatesJson,
                (byte) 0,
                new byte[0]
        );
        subplansAccumulator.add(subplan);

        return new ScanDefinition(exchangeId, columns, predicatesJson);
    }

    private byte[] extractPredicates(RexNode filter) {
        if (filter == null) return new byte[0];

        List<PredicateDTO> dtos = new ArrayList<>();

        if (filter.getKind() == SqlKind.AND) {
            RexCall andCall = (RexCall) filter;
            for (RexNode operand : andCall.getOperands()) {
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
        if (!(call.getOperands().get(1) instanceof org.apache.calcite.rex.RexLiteral literal)) return null;

        int columnIndex = columnRef.getIndex();
        Object value = literal.getValue3();

        String exprType;
        switch (call.getKind()) {
            case EQUALS: exprType = "EQUALS"; break;
            case GREATER_THAN: exprType = "GREATER_THAN"; break;
            case LESS_THAN: exprType = "LESS_THAN"; break;
            case GREATER_THAN_OR_EQUAL: exprType = "GREATER_THAN_OR_EQUALS"; break;
            case LESS_THAN_OR_EQUAL: exprType = "LESS_THAN_OR_EQUALS"; break;
            case NOT_EQUALS: exprType = "NOT_EQUALS"; break;
            default: return null;
        }

        return new PredicateDTO(new ColRefDTO(columnIndex), exprType, value);
    }

    public interface ColumnsResolver {
        List<String> columnsInOrder(String schema, String table);
    }
}