package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorOperatorDefinition;

import java.util.List;


public final class DistributedPlan {
    public final Long snapshot;
    public final List<Object> subPlans;
    public final CoordinatorOperatorDefinition root;

    public DistributedPlan(Long snapshot, List<Object> subPlans, CoordinatorOperatorDefinition root) {
        this.snapshot = snapshot;
        this.subPlans = subPlans;
        this.root = root;
    }
}