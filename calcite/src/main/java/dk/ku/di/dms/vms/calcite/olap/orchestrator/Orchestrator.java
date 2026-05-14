package dk.ku.di.dms.vms.calcite.olap.orchestrator;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.PlanVisualizer;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.DistributedExecutor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;
import org.apache.calcite.rel.RelNode;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
/**
 * Coordinates the two-phase execution of a distributed analytical query:
 * delegates physical plan translation to {@link DistributedPlanner} to
 * produce a {@link DistributedPlan}, then hands it to
 * {@link DistributedExecutor} for pushdown execution against the VMS cluster.
 * Called once per query by {@link dk.ku.di.dms.vms.calcite.service.OlapGatewayService}
 * with a deep-copied Calcite plan and the current snapshot ID.
 */
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
        LOGGER.log(INFO, "vmodbPhysicalPlan before create: " + vmodbPhysicalPlan);

        DistributedPlan distributedPlan = planner.create(vmodbPhysicalPlan, snapshot);

        if (LOGGER.isLoggable(DEBUG)) {
            LOGGER.log(DEBUG, PlanVisualizer.visualize(distributedPlan));
        }

        return executor.execute(distributedPlan);
    }
}