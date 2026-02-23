package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbFilter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

public final class VModbFilterRule extends RelOptRule {

    public static final VModbFilterRule INSTANCE = new VModbFilterRule();

    private VModbFilterRule() {
        super(operand(LogicalFilter.class, any()), "VModbFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        RelNode input = filter.getInput();

        RelNode convertedInput = call.getPlanner().changeTraits(
                input,
                input.getTraitSet().replace(VModbConvention.INSTANCE)
        );

        if (convertedInput == null) return;

        RelNode out = VModbFilter.create(convertedInput, filter.getCondition());
        call.transformTo(out);
    }
}