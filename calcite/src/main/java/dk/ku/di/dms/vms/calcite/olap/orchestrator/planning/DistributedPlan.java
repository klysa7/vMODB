package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorHashJoinOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.NewCoordinatorOperation;

import java.util.List;

public final class DistributedPlan {
    public final Long snapshot;
    public final List<VmsSubplan> subPlans;
    public final CoordinatorHashJoinOperation join;
    public final NewCoordinatorOperation root;

    public DistributedPlan(Long snapshot, List<VmsSubplan> subPlans,
                           CoordinatorHashJoinOperation join, NewCoordinatorOperation root) {
        this.snapshot = snapshot;
        this.subPlans = subPlans;
        this.join = join;
        this.root = root;
    }
}