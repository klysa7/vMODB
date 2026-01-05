package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.GroupByMinMaxEnum;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbGroupByMinMax;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public final class VModbGroupByMinMaxRule extends RelOptRule {

    public static final VModbGroupByMinMaxRule INSTANCE = new VModbGroupByMinMaxRule();

    private VModbGroupByMinMaxRule() {
        super(operand(LogicalAggregate.class, any()), "VModbGroupByMinMaxRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate aggregate = call.rel(0);

        if (aggregate.getGroupSet().isEmpty()) return;

        if (aggregate.getAggCallList().size() != 1) return;

        var aggregateCall = aggregate.getAggCallList().get(0);
        SqlKind k = aggregateCall.getAggregation().getKind();
        GroupByMinMaxEnum kind;
        if (k == SqlKind.MIN) kind = GroupByMinMaxEnum.MIN;
        else if (k == SqlKind.MAX) kind = GroupByMinMaxEnum.MAX;
        else return;

        List<Integer> args = aggregateCall.getArgList();
        if (args.size() != 1) return;
        Integer aggregateColumnIndex = args.get(0);

        int[] groupCols = aggregate.getGroupSet().asList().stream().mapToInt(i -> i).toArray();

        RelNode input = aggregate.getInput();
        RelNode converted = convert(input, input.getTraitSet().replace(VModbConvention.INSTANCE));
        if (converted == null) return;

        call.transformTo(VModbGroupByMinMax.create(converted,
                aggregate.getRowType(), kind, groupCols, aggregateColumnIndex, -1));
    }
}
