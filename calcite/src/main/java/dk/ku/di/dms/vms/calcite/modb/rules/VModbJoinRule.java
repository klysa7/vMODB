package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public final class VModbJoinRule extends ConverterRule {

    public static final VModbJoinRule INSTANCE = new VModbJoinRule();

    private VModbJoinRule() {
        super(
                LogicalJoin.class,
                Convention.NONE,
                VModbConvention.INSTANCE,
                "VModbJoinRule"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        if (join.getJoinType() != JoinRelType.INNER) return false;

        RexNode cond = join.getCondition();
        if (!(cond instanceof RexCall eq)) return false;
        if (eq.getKind() != SqlKind.EQUALS) return false;
        if (eq.operands.size() != 2) return false;

        return (eq.operands.get(0) instanceof RexInputRef) &&
                (eq.operands.get(1) instanceof RexInputRef);
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalJoin join = (LogicalJoin) rel;

        RexCall eq = (RexCall) join.getCondition();
        RexInputRef a = (RexInputRef) eq.operands.get(0);
        RexInputRef b = (RexInputRef) eq.operands.get(1);

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        RelNode leftSubTree =
                convert(left, left.getTraitSet().replace(VModbConvention.INSTANCE));
        RelNode rightSubTree =
                convert(right, right.getTraitSet().replace(VModbConvention.INSTANCE));

        if (leftSubTree.getTraitSet().getConvention() != VModbConvention.INSTANCE) return null;
        if (rightSubTree.getTraitSet().getConvention() != VModbConvention.INSTANCE) return null;

        int leftFieldCount = left.getRowType().getFieldCount();
        int leftJoinCol;
        int rightJoinCol;

        if (a.getIndex() < leftFieldCount) {
            leftJoinCol = a.getIndex();
            rightJoinCol = b.getIndex() - leftFieldCount;
        } else {
            leftJoinCol = b.getIndex();
            rightJoinCol = a.getIndex() - leftFieldCount;
        }

        return VModbJoin.create(leftSubTree, rightSubTree, join.getRowType(), leftJoinCol, rightJoinCol);
    }
}