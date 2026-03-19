package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;

/**
 * A7: Simple scan subplan.
 * A single VMS scans its table and sends rows directly to the gateway.
 * Mode 0.
 */
public record ScanSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        SubPlanOperation operation,
        List<String> columnsInOrder,
        byte[] predicates,
        List<ColumnDescriptor> columnDescriptors) {}