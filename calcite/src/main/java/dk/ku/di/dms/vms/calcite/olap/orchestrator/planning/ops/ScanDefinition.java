package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

import java.util.List;

public record ScanDefinition(
        String exchangeId,
        List<String> outputColumns,
        byte[] predicates,
        int[] projectedIndices    // QPO-3: null = all columns
) implements CoordinatorOperatorDefinition {

    /** Backward-compatible constructor — no projection */
    public ScanDefinition(String exchangeId, List<String> outputColumns, byte[] predicates) {
        this(exchangeId, outputColumns, predicates, null);
    }
}