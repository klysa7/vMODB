package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;
import java.util.List;

public final class VmsSubplan {
    public final String vmsName;
    public final String url;
    public final String exchangeId;
    public final SubPlanOperation operation;
    public final List<String> columnsInOrder;
    public final byte[] predicates;
    public final byte mode;
    public final byte[] routingData;

    public VmsSubplan(String vmsName, String url, String exchangeId,
                      SubPlanOperation operation, List<String> columnsInOrder, byte[] predicates,
                      byte mode, byte[] routingData) {
        this.vmsName = vmsName;
        this.url = url;
        this.exchangeId = exchangeId;
        this.operation = operation;
        this.columnsInOrder = columnsInOrder;
        this.predicates = predicates;
        this.mode = mode; // NEW
        this.routingData = routingData; // NEW
    }
}