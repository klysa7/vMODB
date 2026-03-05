package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;

public final class VmsSubplan {
    public final String vmsName;
    public final String url;
    public final String exchangeId;
    public final SubPlanOperation operation;
    /** Ordered column names in the row this subplan produces. */
    public final List<String> columnsInOrder;
    public final byte[] predicates;
    public final byte mode;
    public final byte[] routingData;

    /**
     * Full column descriptors — name, type, byte offset, byte size — for every column
     * in the row payload this subplan produces.
     *
     * For a simple scan: descriptors cover the single table's columns.
     * For a broadcast join (MODE_RECEIVE_AND_JOIN): descriptors cover the combined
     *   [leftPayload | rightPayload] row, in order, so VmsResultIterator can navigate
     *   the binary row without any Object[] materialization.
     *
     * Null when the subplan is not the receiver of a join (e.g. the broadcast sender).
     */
    public final List<ColumnDescriptor> columnDescriptors;

    /** Constructor used for simple scans or broadcast senders (no descriptor needed). */
    public VmsSubplan(String vmsName, String url, String exchangeId,
                      SubPlanOperation operation, List<String> columnsInOrder,
                      byte[] predicates, byte mode, byte[] routingData) {
        this(vmsName, url, exchangeId, operation, columnsInOrder,
                predicates, mode, routingData, null);
    }

    /** Full constructor including column descriptors for join receivers. */
    public VmsSubplan(String vmsName, String url, String exchangeId,
                      SubPlanOperation operation, List<String> columnsInOrder,
                      byte[] predicates, byte mode, byte[] routingData,
                      List<ColumnDescriptor> columnDescriptors) {
        this.vmsName           = vmsName;
        this.url               = url;
        this.exchangeId        = exchangeId;
        this.operation         = operation;
        this.columnsInOrder    = columnsInOrder;
        this.predicates        = predicates;
        this.mode              = mode;
        this.routingData       = routingData;
        this.columnDescriptors = columnDescriptors;
    }
}