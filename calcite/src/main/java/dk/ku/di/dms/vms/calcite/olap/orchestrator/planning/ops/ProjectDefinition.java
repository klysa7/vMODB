package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public record ProjectDefinition(
        CoordinatorOperatorDefinition input,
        int[] projectedIndices
) implements CoordinatorOperatorDefinition {}