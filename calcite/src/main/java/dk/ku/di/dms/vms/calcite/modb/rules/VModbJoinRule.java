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

import java.util.ArrayList;
import java.util.List;

public final class VModbJoinRule extends RelOptRule {

    public static final VModbJoinRule INSTANCE = new VModbJoinRule();

    private VModbJoinRule() {
        super(operand(LogicalJoin.class, any()), "VModbJoinRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        return join.getJoinType() == JoinRelType.INNER; // Accept any inner join, we'll validate keys in onMatch
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();

        RelNode leftConverted = call.getPlanner().changeTraits(left,
                left.getTraitSet().replace(VModbConvention.INSTANCE));
        RelNode rightConverted = call.getPlanner().changeTraits(right,
                right.getTraitSet().replace(VModbConvention.INSTANCE));
        if (leftConverted == null || rightConverted == null) return;

        List<Integer> leftKeys  = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode condition = join.getCondition();

        if (condition.isAlwaysTrue()) {
            // cross join — empty key lists, executes as nested-loop product
        } else if (condition.getKind() == SqlKind.AND) {
            for (RexNode op : ((RexCall) condition).getOperands()) {
                if (!parseEquality(op, leftKeys, rightKeys, leftFieldCount)) return;
            }
        } else if (condition.getKind() == SqlKind.EQUALS) {
            if (!parseEquality(condition, leftKeys, rightKeys, leftFieldCount)) return;
        } else {
            return; // Reject other non-equi joins
        }

        int[] leftJoinCols  = leftKeys.stream().mapToInt(i -> i).toArray();
        int[] rightJoinCols = rightKeys.stream().mapToInt(i -> i).toArray();

        RelNode out = VModbJoin.create(leftConverted, rightConverted,
                join.getRowType(), leftJoinCols, rightJoinCols);
        call.transformTo(out);
    }

    private boolean parseEquality(RexNode operand, List<Integer> leftKeys, List<Integer> rightKeys, int leftFieldCount) {
        if (operand.getKind() != SqlKind.EQUALS) return false;
        RexCall eqCall = (RexCall) operand;
        if (!(eqCall.operands.get(0) instanceof RexInputRef op1) || !(eqCall.operands.get(1) instanceof RexInputRef op2)) return false;

        int idx1 = op1.getIndex();
        int idx2 = op2.getIndex();

        if (idx1 < leftFieldCount) {
            leftKeys.add(idx1);
            rightKeys.add(idx2 - leftFieldCount);
        } else {
            leftKeys.add(idx2);
            rightKeys.add(idx1 - leftFieldCount);
        }
        return true;
    }
}