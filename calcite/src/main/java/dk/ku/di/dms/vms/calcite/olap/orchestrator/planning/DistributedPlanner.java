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

    public DistributedPlan distribute(Object physicalPlan, long snapshot) {
        if (physicalPlan instanceof RelNode rel) {

            if (!(rel instanceof Project proj)) {
                throw new IllegalArgumentException("Expected Project EnumerableProject but instead" + rel.getClass().getName());
            }
            if (!(proj.getInput() instanceof Join join)) {
                throw new IllegalArgumentException("Expected Project(Join) but instead" + proj.getInput().getClass().getName());
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

            int[] joinKeys = extractEquiJoinKeys(join);
            int leftKey = joinKeys[0];
            int rightKey = joinKeys[1];

            CoordinatorHashJoinOperation joinOp =
                    new CoordinatorHashJoinOperation(left.exchangeId, right.exchangeId, leftKey, rightKey, "ex_joined");

            int[] projects = extractProjectIndices(proj);

            CoordinatorProjectOperation projectOp =
                    new CoordinatorProjectOperation(joinOp.outExchangeId, projects, "ex_final");

            return new DistributedPlan(snapshot, List.of(left, right), joinOp, projectOp);
        }


        if (!(physicalPlan instanceof VModbProject proj)) {
            throw new IllegalArgumentException("Expected VModbProject root, got: " + physicalPlan.getClass().getName());
        }
        if (!(proj.getInput() instanceof VModbJoin join)) {
            throw new IllegalArgumentException("Expected Project(Join(...)), got: " + proj.getInput().getClass().getName());
        }
        if (!(join.getLeft() instanceof VModbTableAccess leftAcc)) {
            throw new IllegalArgumentException("Expected left TableAccess, got: " + join.getLeft().getClass().getName());
        }
        if (!(join.getRight() instanceof VModbTableAccess rightAcc)) {
            throw new IllegalArgumentException("Expected right TableAccess, got: " + join.getRight().getClass().getName());
        }

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

        int leftKey = join.getLeftJoinCol();
        int rightKey = join.getRightJoinCol();

        CoordinatorHashJoinOperation joinOp =
                new CoordinatorHashJoinOperation(left.exchangeId, right.exchangeId, leftKey, rightKey, "ex_joined");

        int[] projects = proj.getProjects();
        CoordinatorProjectOperation projectOp =
                new CoordinatorProjectOperation(joinOp.outExchangeId, projects, "ex_final");

        return new DistributedPlan(snapshot, List.of(left, right), joinOp, projectOp);
    }

    private VModbTableAccess unwrapToTableAccess(RelNode node) {
        RelNode cur = node;

        while (!(cur instanceof VModbTableAccess)) {
            if (cur.getInputs() == null || cur.getInputs().size() != 1) {
                throw new IllegalArgumentException(
                        "Expected a single-input converter wrapping VModbTableAccess, got: " + cur.getClass().getName()
                );
            }
            cur = cur.getInput(0);
        }
        return (VModbTableAccess) cur;
    }

    private int[] extractProjectIndices(Project proj) {
        List<RexNode> exprs = proj.getProjects();
        int[] idx = new int[exprs.size()];

        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) {
                throw new IllegalArgumentException(
                        "v1 only supports simple input-ref projection, got: " + e.getClass().getName() + " expr=" + e
                );
            }
            idx[i] = ref.getIndex();
        }
        return idx;
    }


    private int[] extractEquiJoinKeys(Join join) {
        RexNode cond = join.getCondition();

        if (!(cond instanceof RexCall call) || call.getKind() != SqlKind.EQUALS) {
            throw new IllegalArgumentException("v1 only supports equi-join condition (=), got: " + cond);
        }
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("Unexpected join condition operands: " + call.getOperands().size());
        }

        RexNode a = call.getOperands().get(0);
        RexNode b = call.getOperands().get(1);

        if (!(a instanceof RexInputRef ar) || !(b instanceof RexInputRef br)) {
            throw new IllegalArgumentException("v1 only supports input-ref join keys, got: " + a + " and " + b);
        }

        int aIdx = ar.getIndex();
        int bIdx = br.getIndex();

        int leftCount = join.getLeft().getRowType().getFieldCount();


        boolean aIsRight = aIdx >= leftCount;
        boolean bIsRight = bIdx >= leftCount;

        if (aIsRight == bIsRight) {
            throw new IllegalArgumentException(
                    "Join keys must be one from left and one from right. leftCount=" + leftCount + " cond=" + cond
            );
        }

        int leftKey = aIsRight ? bIdx : aIdx;
        int rightKeyGlobal = aIsRight ? aIdx : bIdx;
        int rightKey = rightKeyGlobal - leftCount;

        return new int[]{leftKey, rightKey};
    }

    public interface ColumnsResolver {
        List<String> columnsInOrder(String schema, String table);
    }
}