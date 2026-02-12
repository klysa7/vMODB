package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorHashJoinOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorOperatorDefinition;

import java.util.List;

public final class DistributedPlan {
    public final Long snapshot;
    public final List<VmsSubplan> subPlans;
    public final CoordinatorOperatorDefinition root;

    public DistributedPlan(Long snapshot, List<VmsSubplan> subPlans, CoordinatorOperatorDefinition root) {
        this.snapshot = snapshot;
        this.subPlans = subPlans;
        this.root = root;
    }
}