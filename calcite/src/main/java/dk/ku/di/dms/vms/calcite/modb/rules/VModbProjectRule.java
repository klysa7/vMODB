package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbProject;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;


public final class VModbProjectRule extends ConverterRule {

    public static final VModbProjectRule INSTANCE = new VModbProjectRule();

    private VModbProjectRule() {
        super(
                LogicalProject.class,
                Convention.NONE,
                VModbConvention.INSTANCE,
                "VModbProjectRule"
        );
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;

        RelNode input = project.getInput();

        RelNode convertedInput =
                convert(input, input.getTraitSet().replace(VModbConvention.INSTANCE));

        if (convertedInput.getTraitSet().getConvention() != VModbConvention.INSTANCE) {
            return null;
        }

        return VModbProject.create(convertedInput, project.getProjects(), project.getRowType());
    }
}
