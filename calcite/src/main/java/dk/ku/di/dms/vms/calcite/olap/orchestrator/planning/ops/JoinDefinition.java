package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public record JoinDefinition(
        CoordinatorOperatorDefinition left,
        CoordinatorOperatorDefinition right,
        Integer leftKeyIndex,
        Integer rightKeyIndex
) implements CoordinatorOperatorDefinition {}
