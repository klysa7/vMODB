package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbJoin;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbProject;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorHashJoinOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorProjectOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.List;


public final class DistributedPlanner {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedPlanner.class.getName());

    private final PlacementResolver placement;
    private final ColumnsResolver columnsResolver;

    public DistributedPlanner(PlacementResolver placement, ColumnsResolver columnsResolver) {
        this.placement = placement;
        this.columnsResolver = columnsResolver;
    }

    public DistributedPlan distribute(Object physicalPlan, Long snapshot) {

        if (physicalPlan instanceof RelNode rel) {

            if (!(rel instanceof Project project)) {
                throw new IllegalArgumentException("Expected Project EnumerableProject but instead" + rel.getClass().getName());
            }
            if (!(project.getInput() instanceof Join join)) {
                throw new IllegalArgumentException("Expected Project(Join) but instead" + project.getInput().getClass().getName());
            }

            VModbTableAccess leftAcc = unwrapToTableAccess(join.getLeft());
            VModbTableAccess rightAcc = unwrapToTableAccess(join.getRight());

            String leftSchema = leftAcc.getSchemaName();
            String leftTable  = leftAcc.getTableName();

            String rightSchema = rightAcc.getSchemaName();
            String rightTable  = rightAcc.getTableName();

            VmsSubplan left = new VmsSubplan(
                    placement.ownerVms(leftSchema, leftTable),
                    placement.endpointUrl(leftSchema, leftTable),
                    "ex_left",
                    new ScanAllOperation(leftSchema, leftTable),
                    columnsResolver.columnsInOrder(leftSchema, leftTable)
            );

            VmsSubplan right = new VmsSubplan(
                    placement.ownerVms(rightSchema, rightTable),
                    placement.endpointUrl(rightSchema, rightTable),
                    "ex_right",
                    new ScanAllOperation(rightSchema, rightTable),
                    columnsResolver.columnsInOrder(rightSchema, rightTable)
            );

            int[] joinKeys = extractJoinKeys(join);
            int leftKey = joinKeys[0];
            int rightKey = joinKeys[1];

            CoordinatorHashJoinOperation joinOperation =
                    new CoordinatorHashJoinOperation(left.exchangeId, right.exchangeId, leftKey, rightKey, "ex_joined");

            int[] projects = extractProjectIndices(project);

            CoordinatorProjectOperation projectOperation =
                    new CoordinatorProjectOperation(joinOperation.outExchangeId, projects, "ex_final");

            return new DistributedPlan(snapshot, List.of(left, right), joinOperation, projectOperation);
        }
        else {
            throw new IllegalArgumentException("Unsupported plan");
        }
    }

    private VModbTableAccess unwrapToTableAccess(RelNode node) {
        RelNode relNode = node;

        while (!(relNode instanceof VModbTableAccess)) {
            if (relNode.getInputs() == null || relNode.getInputs().size() != 1) {
                throw new IllegalArgumentException("Expected a single input converter that has VModbTableAccess ");
            }
            relNode = relNode.getInput(0);
        }
        return (VModbTableAccess) relNode;
    }

    private int[] extractProjectIndices(Project project) {
        List<RexNode> expressions = project.getProjects();
        int[] idx = new int[expressions.size()];

        for (int i = 0; i < expressions.size(); i++) {
            RexNode e = expressions.get(i);
            if (!(e instanceof RexInputRef ref)) {
                throw new IllegalArgumentException("only rexinputref supportwd ");
            }
            idx[i] = ref.getIndex();
        }
        return idx;
    }


    private int[] extractJoinKeys(Join join) {
        RexNode condition = join.getCondition();

        if (!(condition instanceof RexCall call) || call.getKind() != SqlKind.EQUALS) {
            throw new IllegalArgumentException("only supports equal join condition");
        }
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("unexpected join condition operands");
        }

        RexNode leftSide = call.getOperands().get(0);
        RexNode rightSide = call.getOperands().get(1);

        if (!(leftSide instanceof RexInputRef rexInputRefLeft) || !(rightSide instanceof RexInputRef rexInputRefRight)) {
            throw new IllegalArgumentException("v1 only supports input-ref join keys, got: " + leftSide + " and " + rightSide);
        }

        int border = join.getLeft().getRowType().getFieldCount();

        boolean aIsRight = rexInputRefLeft.getIndex() >= border;
        boolean bIsRight = rexInputRefRight.getIndex() >= border;
        if (aIsRight == bIsRight) {
            throw new IllegalArgumentException("Join keys validation failed they must be from different VMSes");
        }

        int leftKey = aIsRight ? rexInputRefRight.getIndex() : rexInputRefLeft.getIndex();
        int rightKeyGlobal = aIsRight ? rexInputRefLeft.getIndex() : rexInputRefRight.getIndex();
        int rightKey = rightKeyGlobal - border;

        return new int[]{leftKey, rightKey};
    }

    public interface ColumnsResolver {
        List<String> columnsInOrder(String schema, String table);
    }
}