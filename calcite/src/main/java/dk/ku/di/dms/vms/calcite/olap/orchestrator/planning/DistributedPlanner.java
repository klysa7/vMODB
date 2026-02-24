package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbFilter;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbJoin;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbProject;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
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

        // We know the filter is directly on top of the scan now!
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

    // Lightweight DTOs to mimic the VMS WherePredicate structure
    public record ColRefDTO(int columnPosition) {}
    public record PredicateDTO(ColRefDTO columnReference, String expression, Object value) {}

    private ScanDefinition createScanSubplan(VModbTableAccess scan, RexNode condition) {
        String schema = scan.getSchemaName();
        String table = scan.getTableName();
        String exchangeId = "exchange_" + (exchangeCounter++);

        List<String> columns = columnsResolver.columnsInOrder(schema, table);

        // We use the decoupled condition!
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
        Object value = literal.getValue3(); // Extracts raw primitive/String from Calcite literal

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


    //for if we  find converters
    private boolean isTableAccess(RelNode node) {
        if(node instanceof VModbTableAccess) return true;
        if(node.getInputs().size() == 1) return isTableAccess(node.getInput(0));
        return false;
    }

    private VModbTableAccess unwrapToTableAccess(RelNode node) {
        if (node instanceof VModbTableAccess access) return access;
        return unwrapToTableAccess(node.getInput(0));
    }

    private int[] extractProjectIndices(Project project) {
        List<RexNode> expressions = project.getProjects();
        int[] idx = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            idx[i] = ((RexInputRef) expressions.get(i)).getIndex();
        }
        return idx;
    }


    private record JoinKeys(int[] left, int[] right) {}

    private JoinKeys extractJoinKeys(Join join) {
        RexNode condition = join.getCondition();
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();

        if (condition.getKind() == SqlKind.AND) {
            RexCall andCall = (RexCall) condition;
            for (RexNode operand : andCall.getOperands()) {
                parseEquality(operand, leftKeys, rightKeys, leftFieldCount);
            }
        }
        else if (condition.getKind() == SqlKind.EQUALS) {
            parseEquality(condition, leftKeys, rightKeys, leftFieldCount);
        }
        else {
            throw new IllegalArgumentException("Gateway error: only supports equal join condition (found " + condition.getKind() + ")");
        }

        return new JoinKeys(
                leftKeys.stream().mapToInt(i -> i).toArray(),
                rightKeys.stream().mapToInt(i -> i).toArray()
        );
    }

    private void parseEquality(RexNode operand, List<Integer> leftKeys, List<Integer> rightKeys, int leftFieldCount) {
        if (operand.getKind() != SqlKind.EQUALS) {
            throw new RuntimeException("Gateway error: inside AND, only supports equal join condition");
        }
        RexCall eqCall = (RexCall) operand;
        RexInputRef op1 = (RexInputRef) eqCall.getOperands().get(0);
        RexInputRef op2 = (RexInputRef) eqCall.getOperands().get(1);

        int idx1 = op1.getIndex();
        int idx2 = op2.getIndex();

        if (idx1 < leftFieldCount) {
            leftKeys.add(idx1);
            rightKeys.add(idx2 - leftFieldCount);
        } else {
            leftKeys.add(idx2);
            rightKeys.add(idx1 - leftFieldCount);
        }
    }

    public interface ColumnsResolver {
        List<String> columnsInOrder(String schema, String table);
    }
}