package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;

/**
 * A7: Simple scan subplan.
 * A single VMS scans its table and sends rows directly to the gateway.
 * Mode 0.
 *
 * QPO-3: projectionData carries the serialized int[] of column indices
 * to request from the VMS. null means full scan (all columns).
 * Computed by DistributedExecutor from VModbTableAccess.projects.
 */
public record ScanSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        SubPlanOperation operation,
        List<String> columnsInOrder,
        byte[] predicates,
        List<ColumnDescriptor> columnDescriptors,
        byte[] projectionData) {      // QPO-3: new field

    /** Backward-compatible constructor — no projection (full scan) */
    public ScanSubPlan(String vmsName, String url, String exchangeId,
                       SubPlanOperation operation, List<String> columnsInOrder,
                       byte[] predicates, List<ColumnDescriptor> columnDescriptors) {
        this(vmsName, url, exchangeId, operation, columnsInOrder,
                predicates, columnDescriptors, null);
    }
}