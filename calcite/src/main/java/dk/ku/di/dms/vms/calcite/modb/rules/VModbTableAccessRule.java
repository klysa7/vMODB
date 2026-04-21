package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Converts LogicalTableScan → VModbTableAccess.
 *
 * QPO-3 extension: also matches LogicalProject(LogicalTableScan) and folds
 * the projection into VModbTableAccess.projects. This means:
 *   - The VMS only serializes the projected columns (not all 10 for order_line)
 *   - DistributedExecutor reads projects from ScanDefinition.projectedIndices()
 *   - No separate VModbProject node is needed for simple column projections
 *
 * Two patterns are registered:
 *   Pattern 1: LogicalTableScan alone         → projects = null (all columns)
 *   Pattern 2: LogicalProject(LogicalTableScan)→ projects = extracted indices
 */
public final class VModbTableAccessRule extends RelOptRule {

    /** Pattern 1: plain table scan — no projection */
    public static final VModbTableAccessRule INSTANCE =
            new VModbTableAccessRule();

    /** Pattern 2: project on top of table scan — fold into VModbTableAccess */
    public static final VModbTableAccessRule PROJECT_INSTANCE =
            new VModbTableAccessRule(true);

    private final boolean withProject;

    /** Plain scan rule */
    private VModbTableAccessRule() {
        super(operand(LogicalTableScan.class, any()), "VModbTableScanRule");
        this.withProject = false;
    }

    /** Project + scan rule */
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

    // ── Pattern 1: LogicalTableScan → VModbTableAccess (projects = null) ──────

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
                null,   // projects = ALL columns (no pushdown)
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

    // ── Pattern 2: LogicalProject(LogicalTableScan) → VModbTableAccess ────────
    //
    // QPO-3: fold the project into the table access so the VMS only sends
    // the projected columns. For CHQ6 this reduces transfer from ~200 bytes/row
    // to 4 bytes/row (ol_amount only).
    //
    // Only works for simple column references (RexInputRef). Computed expressions
    // (e.g. CAST, arithmetic) are not supported and fall through to the
    // separate VModbProjectRule.

    private void matchProjectScan(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalTableScan scan  = call.rel(1);

        if (scan.getTraitSet().getConvention() == VModbConvention.INSTANCE) return;

        // Try to extract simple column indices from the projection
        int[] projectedIndices = tryExtractInputRefs(project.getProjects());
        if (projectedIndices == null) {
            // Contains computed expressions — cannot fold; let VModbProjectRule handle it
            return;
        }

        List<String> qualified = scan.getTable().getQualifiedName();
        String schema = qualified.size() >= 2
                ? qualified.get(qualified.size() - 2) : "default";

        // Create VModbTableAccess with the projected column indices
        // deriveRowType() uses projects[] to return only the projected fields
        RelNode out = VModbTableAccess.create(
                scan.getCluster(),
                scan.getTable(),
                schema,
                projectedIndices,   // QPO-3: only these column indices
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

    /**
     * Extracts column indices from a list of projection expressions.
     * Returns null if any expression is not a simple column reference
     * (i.e. the projection cannot be folded into a table scan).
     */
    private static int[] tryExtractInputRefs(List<RexNode> exprs) {
        int[] indices = new int[exprs.size()];
        for (int i = 0; i < exprs.size(); i++) {
            RexNode e = exprs.get(i);
            if (!(e instanceof RexInputRef ref)) {
                return null; // computed expression — cannot fold
            }
            indices[i] = ref.getIndex();
        }
        return indices;
    }
}