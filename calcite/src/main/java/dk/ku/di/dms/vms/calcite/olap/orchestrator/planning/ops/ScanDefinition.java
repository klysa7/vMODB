package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

import java.util.List;

public record ScanDefinition(
        String exchangeId,
        List<String> outputColumns,
        byte[] predicates
) implements CoordinatorOperatorDefinition {}