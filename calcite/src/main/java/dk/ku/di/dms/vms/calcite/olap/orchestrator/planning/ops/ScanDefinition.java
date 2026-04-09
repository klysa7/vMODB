package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

import java.util.List;

/**
 * QPO-3: projectedIndices carries the int[] of column indices to request
 * from the VMS. null means full scan (all columns — backward compatible).
 * Populated by DistributedPlanner from VModbTableAccess.projects.
 */
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