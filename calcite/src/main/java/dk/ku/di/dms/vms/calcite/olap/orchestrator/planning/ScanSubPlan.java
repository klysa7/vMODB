package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;


public record ScanSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        SubPlanOperation operation,
        List<String> columnsInOrder,
        byte[] predicates,
        List<ColumnDescriptor> columnDescriptors,
        byte[] projectionData) {

    public ScanSubPlan(String vmsName, String url, String exchangeId,
                       SubPlanOperation operation, List<String> columnsInOrder,
                       byte[] predicates, List<ColumnDescriptor> columnDescriptors) {
        this(vmsName, url, exchangeId, operation, columnsInOrder,
                predicates, columnDescriptors, null);
    }
}