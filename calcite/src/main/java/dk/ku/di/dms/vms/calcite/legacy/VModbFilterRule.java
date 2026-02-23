package dk.ku.di.dms.vms.calcite.legacy;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;

public final class VModbFilterRule extends ConverterRule {

    public static final VModbFilterRule INSTANCE = new VModbFilterRule();

    private VModbFilterRule() {
        super(
                LogicalFilter.class,
                Convention.NONE,
                VModbConvention.INSTANCE,
                "VModbFilterRule"
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter f = (LogicalFilter) rel;

        RelNode input = f.getInput();
        RelNode vInput = convert(input, input.getTraitSet().replace(VModbConvention.INSTANCE));
        if (vInput == null || vInput.getTraitSet().getConvention() != VModbConvention.INSTANCE) return null;

        return VModbFilter.create(vInput, f.getCondition());
    }
}