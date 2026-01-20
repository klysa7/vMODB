package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;

public final class VmsSubplan {
    public final String vmsName;
    public final String url;
    public final String exchangeId;
    public final SubPlanOperation operation;
    public final List<String> columnsInOrder;

    public VmsSubplan(String vmsName, String url, String exchangeId,
                      SubPlanOperation operation, List<String> columnsInOrder) {
        this.vmsName = vmsName;
        this.url = url;
        this.exchangeId = exchangeId;
        this.operation = operation;
        this.columnsInOrder = columnsInOrder;
    }
}