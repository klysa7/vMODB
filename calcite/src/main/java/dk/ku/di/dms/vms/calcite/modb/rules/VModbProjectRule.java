package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbProject;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public final class VModbProjectRule extends RelOptRule {

    public static final VModbProjectRule INSTANCE = new VModbProjectRule();

    private VModbProjectRule() {
        super(operand(LogicalProject.class, any()), "VModbProjectRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        RelNode input = project.getInput();

        RelNode convertedInput = call.getPlanner().changeTraits(
                input,
                input.getTraitSet().replace(VModbConvention.INSTANCE)
        );
        if (convertedInput == null) return;


        if (convertedInput instanceof VModbTableAccess tableAccess
                && tableAccess.projects == null) {
            int[] projectedIndices = tryExtractInputRefs(project.getProjects());
            if (projectedIndices != null) {
                RelNode folded = VModbTableAccess.create(
                        tableAccess.getCluster(),
                        tableAccess.getTable(),
                        tableAccess.getSchemaName(),
                        projectedIndices,
                        tableAccess.getFilter(),
                        tableAccess.getKey(),
                        tableAccess.getKeys()
                );
                if (folded.getTraitSet().getConvention() != VModbConvention.INSTANCE) {
                    folded = folded.copy(
                            folded.getTraitSet().replace(VModbConvention.INSTANCE),
                            folded.getInputs()
                    );
                }
                call.transformTo(folded);
                return;
            }
        }

        int[] projects = requireInputRefs(project.getProjects());
        RelNode out = VModbProject.create(convertedInput, project.getRowType(), projects);
        call.transformTo(out);
    }

    private static int[] tryExtractInputRefs(List<RexNode> exprs) {
        int[] out = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) return null;
            out[i] = ref.getIndex();
        }
        return out;
    }

    private static int[] requireInputRefs(List<RexNode> exprs) {
        int[] out = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) {
                throw new UnsupportedOperationException(
                        "Only simple column projections supported: " + e);
            }
            out[i] = ref.getIndex();
        }
        return out;
    }
}