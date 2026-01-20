package dk.ku.di.dms.vms.calcite.olap.orchestrator;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.DistributedExecutor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;

public final class Orchestrator {

    private final DistributedPlanner planner;
    private final DistributedExecutor executor;

    public Orchestrator(DistributedPlanner planner, DistributedExecutor executor) {
        this.planner = planner;
        this.executor = executor;
    }

    public PushdownResponse execute(Object vmodbPhysicalPlan, Long snapshot) {
        DistributedPlan dp = planner.distribute(vmodbPhysicalPlan, snapshot);
        return executor.execute(dp);
    }
}