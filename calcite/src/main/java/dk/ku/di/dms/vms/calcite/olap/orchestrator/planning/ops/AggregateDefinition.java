package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

import java.util.List;

/**
 * Coordinator-side aggregate operator definition.
 * Produced by DistributedPlanner when a VModbAggregate node is encountered.
 *
 * groupByIndices  - column positions in the input row to group by
 * aggCalls        - ordered list of aggregate functions to apply
 */
public record AggregateDefinition(
        CoordinatorOperatorDefinition input,
        int[] groupByIndices,
        List<AggCallDef> aggCalls
) implements CoordinatorOperatorDefinition {

    /**
     * Describes a single aggregate function.
     *
     * kind     - "COUNT", "SUM", "AVG", "MIN", "MAX"
     * argIndex - column index in the input row to aggregate over;
     *            -1 means COUNT(*) (count all rows, no column argument)
     */
    public record AggCallDef(String kind, int argIndex) {}
}