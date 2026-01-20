package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbToEnumerableConverter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

public final class VModbToEnumerableRule extends RelOptRule {

    public static final VModbToEnumerableRule INSTANCE = new VModbToEnumerableRule();

    private VModbToEnumerableRule() {
        super(operand(RelNode.class, any()), "VModbToEnumerableRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);

        if (rel.getTraitSet().getConvention() != VModbConvention.INSTANCE) return;
        if (rel instanceof VModbToEnumerableConverter) return;

        // Wrap any VModbConvention subtree into Enumerable
        call.transformTo(VModbToEnumerableConverter.create(rel));
    }
}