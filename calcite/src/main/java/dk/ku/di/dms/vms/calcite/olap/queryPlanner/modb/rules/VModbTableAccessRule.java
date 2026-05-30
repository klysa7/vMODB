package dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rules;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.modb.rel.VModbTableAccess;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public final class VModbTableAccessRule extends RelOptRule {

    public static final VModbTableAccessRule INSTANCE =
            new VModbTableAccessRule();
    public static final VModbTableAccessRule PROJECT_INSTANCE =
            new VModbTableAccessRule(true);
    private final boolean withProject;

    private VModbTableAccessRule() {
        super(operand(LogicalTableScan.class, any()), "VModbTableScanRule");
        this.withProject = false;
    }

    private VModbTableAccessRule(boolean withProject) {
        super(operand(LogicalProject.class,
                        operand(LogicalTableScan.class, any())),
                "VModbTableScanWithProjectRule");
        this.withProject = true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (withProject) {
            matchProjectScan(call);
        } else {
            matchPlainScan(call);
        }
    }

    private void matchPlainScan(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);

        if (scan.getTraitSet().getConvention() == VModbConvention.INSTANCE) return;

        List<String> qualified = scan.getTable().getQualifiedName();
        String schema = qualified.size() >= 2
                ? qualified.get(qualified.size() - 2) : "default";

        RelNode out = VModbTableAccess.create(
                scan.getCluster(),
                scan.getTable(),
                schema,
                null,
                null,
                null,
                null
        );

        if (out.getTraitSet().getConvention() != VModbConvention.INSTANCE) {
            out = out.copy(
                    out.getTraitSet().replace(VModbConvention.INSTANCE),
                    out.getInputs()
            );
        }

        call.transformTo(out);
    }

    private void matchProjectScan(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalTableScan scan  = call.rel(1);

        if (scan.getTraitSet().getConvention() == VModbConvention.INSTANCE) return;

        int[] projectedIndices = tryExtractInputRefs(project.getProjects());
        if (projectedIndices == null) {
            return;
        }

        List<String> qualified = scan.getTable().getQualifiedName();
        String schema = qualified.size() >= 2
                ? qualified.get(qualified.size() - 2) : "default";

        RelNode out = VModbTableAccess.create(
                scan.getCluster(),
                scan.getTable(),
                schema,
                projectedIndices,
                null,
                null,
                null
        );

        if (out.getTraitSet().getConvention() != VModbConvention.INSTANCE) {
            out = out.copy(
                    out.getTraitSet().replace(VModbConvention.INSTANCE),
                    out.getInputs()
            );
        }

        call.transformTo(out);
    }

    private static int[] tryExtractInputRefs(List<RexNode> exprs) {
        int[] indices = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) {
                return null;
            }
            indices[i] = ref.getIndex();
        }
        return indices;
    }
}