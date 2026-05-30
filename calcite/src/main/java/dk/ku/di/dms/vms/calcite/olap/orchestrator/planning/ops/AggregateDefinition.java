package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

import java.util.List;


public record AggregateDefinition(
        CoordinatorOperatorDefinition input,
        int[] groupByIndices,
        List<AggCallDef> aggCalls
) implements CoordinatorOperatorDefinition {

    public record AggCallDef(String kind, int argIndex) {}
}