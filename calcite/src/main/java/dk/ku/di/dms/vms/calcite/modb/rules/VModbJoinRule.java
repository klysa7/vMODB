package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbJoin;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import org.apache.calcite.plan.RelOptRule;

public final class VModbJoinRule extends RelOptRule {

    public static final VModbJoinRule INSTANCE = new VModbJoinRule();

    private VModbJoinRule() {
        super(operand(LogicalJoin.class, any()), "VModbJoinRule");
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
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        RexCall eq = (RexCall) join.getCondition();
        RexInputRef a = (RexInputRef) eq.operands.get(0);
        RexInputRef b = (RexInputRef) eq.operands.get(1);

        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        RelNode leftConverted = call.getPlanner().changeTraits(
                left,
                left.getTraitSet().replace(VModbConvention.INSTANCE)
        );
        RelNode rightConverted = call.getPlanner().changeTraits(
                right,
                right.getTraitSet().replace(VModbConvention.INSTANCE)
        );
        if (leftConverted == null || rightConverted == null) return;

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

        RelNode out = VModbJoin.create(leftConverted, rightConverted, join.getRowType(), leftJoinCol, rightJoinCol);
        call.transformTo(out);
    }
}