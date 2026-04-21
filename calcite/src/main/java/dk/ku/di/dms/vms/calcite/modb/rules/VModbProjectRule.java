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

/**
 * Converts LogicalProject → VModbProject.
 *
 * QPO-3 extension: when the project's input is already a VModbTableAccess
 * (with projects=null), fold the projection into the table access instead of
 * creating a separate VModbProject. This ensures projection pushdown happens
 * even when VModbTableAccessRule.PROJECT_INSTANCE fires after convention
 * conversion rather than before.
 *
 * If the input is not a VModbTableAccess, or contains computed expressions,
 * falls back to the original VModbProject wrapping behaviour.
 */
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

        // QPO-3: if the converted input is a VModbTableAccess with no existing
        // projection, try to fold this project into it directly.
        // This eliminates a separate VModbProject operator and ensures
        // VModbTableAccess.projects is populated so DistributedPlanner can
        // extract projectedIndices for DistributedExecutor.
        if (convertedInput instanceof VModbTableAccess tableAccess
                && tableAccess.projects == null) {
            int[] projectedIndices = tryExtractInputRefs(project.getProjects());
            if (projectedIndices != null) {
                // QPO-3: fold — create a new VModbTableAccess with projects set
                RelNode folded = VModbTableAccess.create(
                        tableAccess.getCluster(),
                        tableAccess.getTable(),
                        tableAccess.getSchemaName(),
                        projectedIndices,   // QPO-3: projected columns
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
            // else: computed expressions — fall through to VModbProject below
        }

        // Original behaviour: wrap in VModbProject
        int[] projects = requireInputRefs(project.getProjects());
        RelNode out = VModbProject.create(convertedInput, project.getRowType(), projects);
        call.transformTo(out);
    }

    /**
     * Extracts column indices. Returns null if any expression is not a
     * simple column reference (computed expression — cannot fold into scan).
     */
    private static int[] tryExtractInputRefs(List<RexNode> exprs) {
        int[] out = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) return null;
            out[i] = ref.getIndex();
        }
        return out;
    }

    /**
     * Strict version — throws on computed expressions.
     * Used when we know we're creating a VModbProject (not folding into scan).
     */
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