package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public record JoinDefinition(
        CoordinatorOperatorDefinition left,
        CoordinatorOperatorDefinition right,
        int[] leftKeys,
        int[] rightKeys
) implements CoordinatorOperatorDefinition { }