package dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rel.VModbAggregate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;

public final class VModbAggregateRule extends RelOptRule {

    public static final VModbAggregateRule INSTANCE = new VModbAggregateRule();

    private VModbAggregateRule() {
        super(operand(LogicalAggregate.class, any()), "VModbAggregateRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        RelNode input = agg.getInput();

        RelNode convertedInput = call.getPlanner().changeTraits(
                input,
                input.getTraitSet().replace(VModbConvention.INSTANCE));
        if (convertedInput == null) return;

        RelNode out = VModbAggregate.create(
                convertedInput,
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
        call.transformTo(out);
    }
}