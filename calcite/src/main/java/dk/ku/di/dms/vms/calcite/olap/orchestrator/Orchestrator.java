package dk.ku.di.dms.vms.calcite.olap.orchestrator;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.PlanVisualizer;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.DistributedExecutor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;
import org.apache.calcite.rel.RelNode;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public final class Orchestrator {

    private static final System.Logger LOGGER =
            System.getLogger(Orchestrator.class.getName());

    private final DistributedPlanner planner;
    private final DistributedExecutor executor;

    public Orchestrator(DistributedPlanner planner, DistributedExecutor executor) {
        this.planner  = planner;
        this.executor = executor;
    }

    public PushdownResponse execute(RelNode vmodbPhysicalPlan, Long snapshot) {
        // Keep INFO log for activity visibility — cheap (no tree traversal)
        LOGGER.log(INFO, "vmodbPhysicalPlan before create: " + vmodbPhysicalPlan);

        DistributedPlan distributedPlan = planner.create(vmodbPhysicalPlan, snapshot);

        // B33 FIX: gate PlanVisualizer behind DEBUG.
        // BEFORE: LOGGER.log(INFO, PlanVisualizer.visualize(distributedPlan))
        //   PlanVisualizer.visualize() walks the full DistributedPlan tree and
        //   builds a multi-line string on EVERY query execution — even after
        //   QPO-1 means the plan never changes between calls.
        //   Cost: O(plan depth) string allocation + log I/O per query.
        //   Under HATtrick α=2: two concurrent threads building this string
        //   simultaneously, adding GC pressure during measurement window.
        // AFTER: isLoggable(DEBUG) — PlanVisualizer.visualize() never called
        //   at INFO level. Zero allocation, zero I/O during benchmarks.
        //   To see the plan during development: set log level to DEBUG.
        if (LOGGER.isLoggable(DEBUG)) {
            LOGGER.log(DEBUG, PlanVisualizer.visualize(distributedPlan));
        }

        return executor.execute(distributedPlan);
    }
}