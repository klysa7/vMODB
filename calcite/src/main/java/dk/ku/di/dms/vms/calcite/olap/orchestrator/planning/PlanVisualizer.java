package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorOperatorDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.JoinDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ProjectDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.AggregateDefinition;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.Arrays;

public class PlanVisualizer {

    public static String visualizerCalcite(RelNode rel) {
        return RelOptUtil.toString(rel);
    }

    public static String visualize(DistributedPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================ DISTRIBUTED PLAN (Snapshot: ").append(plan.snapshot).append(") ================\n");

        // A7 FIX: subPlans is now List<Object> holding ScanSubplan | JoinSubplan | BroadcastSubplan.
        // Pattern-match on each concrete type instead of casting to the old VmsSubplan.
        sb.append("--- [PHASE 1] REMOTE VMS INSTRUCTIONS ---\n");
        for (Object sub : plan.subPlans) {
            if (sub instanceof ScanSubPlan s) {
                sb.append(String.format("  [%s] SCAN -> VMS: %s (%s)%n", s.exchangeId(), s.vmsName(), s.url()));
                sb.append(String.format("      Op: %s, Cols: %s%n",
                        s.operation().getClass().getSimpleName(), s.columnsInOrder()));
            } else if (sub instanceof JoinSubPlan j) {
                sb.append(String.format("  [%s] JOIN-RECEIVE -> VMS: %s (%s)%n", j.exchangeId(), j.vmsName(), j.url()));
                sb.append(String.format("      Op: %s, Cols: %s%n",
                        j.operation().getClass().getSimpleName(), j.columnsInOrder()));
            } else if (sub instanceof BroadcastSubPlan b) {
                sb.append(String.format("  [%s] BROADCAST -> VMS: %s (%s) target: %s%n",
                        b.exchangeId(), b.vmsName(), b.url(), b.targetAddress()));
            } else {
                sb.append(String.format("  [?] UNKNOWN subplan type: %s%n",
                        sub == null ? "null" : sub.getClass().getSimpleName()));
            }
        }

        sb.append("\n--- [PHASE 2] LOCAL COORDINATOR EXECUTION TREE ---\n");
        printRecursive(plan.root, 0, sb);
        sb.append("==============================================================================\n");

        return sb.toString();
    }

    private static void printRecursive(CoordinatorOperatorDefinition op, int level, StringBuilder sb) {
        String indent = "    ".repeat(level);
        String branch = level == 0 ? "" : "|-- ";
        sb.append(indent).append(branch);

        if (op instanceof JoinDefinition join) {
            sb.append("LOCAL JOIN [Hash]\n");
            sb.append(indent).append("    ")
                    .append("Condition: Left($").append(Arrays.toString(join.leftKeys()))
                    .append(") == Right($").append(Arrays.toString(join.rightKeys())).append(")\n");
            printRecursive(join.left(), level + 1, sb);
            printRecursive(join.right(), level + 1, sb);

        } else if (op instanceof ProjectDefinition proj) {
            sb.append("LOCAL PROJECT [Indices: ").append(Arrays.toString(proj.projectedIndices())).append("]\n");
            printRecursive(proj.input(), level + 1, sb);

        } else if (op instanceof AggregateDefinition agg) {
            // Q4 FIX: AggregateDefinition was falling to UNKNOWN OP before
            sb.append("LOCAL AGGREGATE [GROUP BY: ").append(Arrays.toString(agg.groupByIndices()))
                    .append(" | calls: ").append(agg.aggCalls()).append("]\n");
            printRecursive(agg.input(), level + 1, sb);

        } else if (op instanceof ScanDefinition scan) {
            sb.append("GATHER [ExchangeID: ").append(scan.exchangeId()).append("] ");
            sb.append("(Schema: ").append(scan.outputColumns()).append(")\n");

        } else {
            sb.append("UNKNOWN OP: ").append(op.getClass().getSimpleName()).append("\n");
        }
    }
}