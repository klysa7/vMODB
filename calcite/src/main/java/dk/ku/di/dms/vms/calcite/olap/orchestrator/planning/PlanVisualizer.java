package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;

import java.util.Arrays;

public class PlanVisualizer {

    /**
     * Converts the Calcite RelNode to a standard string representation.
     */
    public static String visualizerCalcite(RelNode rel) {
        return RelOptUtil.toString(rel);
    }

    /**
     * Converts your custom DistributedPlan into a human-readable ASCII tree.
     */
    public static String visualize(DistributedPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n================ DISTRIBUTED PLAN (Snapshot: ").append(plan.snapshot).append(") ================\n");

        // 1. Print the Remote Instructions (The "VmsSubplans")
        sb.append("--- [PHASE 1] REMOTE VMS INSTRUCTIONS ---\n");
        for (VmsSubplan sub : plan.subPlans) {
            sb.append(String.format("  [%s] -> VMS: %s (%s)\n", sub.exchangeId, sub.vmsName, sub.url));
            sb.append(String.format("      Op: %s, Pushdown Cols: %s\n",
                    sub.operation.getClass().getSimpleName(),
                    sub.columnsInOrder));
        }

        // 2. Print the Local Execution Tree (The "Coordinator Operators")
        sb.append("\n--- [PHASE 2] LOCAL COORDINATOR EXECUTION TREE ---\n");
        printRecursive(plan.root, 0, sb);
        sb.append("==============================================================================\n");

        return sb.toString();
    }

    private static void printRecursive(CoordinatorOperatorDefinition op, int level, StringBuilder sb) {
        // Create indentation (e.g., "|   |   ")
        String indent = "    ".repeat(level);
        String branch = level == 0 ? "" : "|-- ";

        sb.append(indent).append(branch);

        if (op instanceof JoinDefinition join) {
            sb.append("LOCAL JOIN [Hash]\n");
            sb.append(indent).append("    ").append("Condition: Left($").append(join.leftKeys())
                    .append(") == Right($").append(join.rightKeys()).append(")\n");

            // Recurse Left and Right
            printRecursive(join.left(), level + 1, sb);
            printRecursive(join.right(), level + 1, sb);

        } else if (op instanceof ProjectDefinition proj) {
            sb.append("LOCAL PROJECT [Indices: ").append(Arrays.toString(proj.projectedIndices())).append("]\n");
            // Recurse Input
            printRecursive(proj.input(), level + 1, sb);

        } else if (op instanceof ScanDefinition scan) {
            sb.append("GATHER [ExchangeID: ").append(scan.exchangeId()).append("] ");
            sb.append("(Schema: ").append(scan.outputColumns()).append(")\n");

        } else {
            sb.append("UNKNOWN OP: ").append(op.getClass().getSimpleName()).append("\n");
        }
    }
}
