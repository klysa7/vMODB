package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorOperatorDefinition;

import java.util.List;

/**
 * A7: subPlans now holds a plain Object list because the three subplan types
 * (ScanSubplan, JoinSubplan, BroadcastSubplan) do not share a common interface.
 * DistributedExecutor pattern-matches on the concrete type.
 *
 * Alternatively you could introduce a sealed interface SubplanMarker —
 * but Object is simpler and avoids an extra file while still being type-safe
 * at the pattern-match sites.
 */
public final class DistributedPlan {
    public final Long snapshot;
    public final List<Object> subPlans; // ScanSubplan | JoinSubplan | BroadcastSubplan
    public final CoordinatorOperatorDefinition root;

    public DistributedPlan(Long snapshot, List<Object> subPlans, CoordinatorOperatorDefinition root) {
        this.snapshot = snapshot;
        this.subPlans = subPlans;
        this.root = root;
    }
}