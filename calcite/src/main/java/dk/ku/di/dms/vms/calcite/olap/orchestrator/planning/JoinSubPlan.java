package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;

/**
 * A7: Probe side subplan.
 * The Order VMS receives the broadcast, builds the hash map, joins against
 * its local table, and sends combined rows back to the gateway.
 * Mode 2.
 */
public record JoinSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        SubPlanOperation operation,
        List<String> columnsInOrder,
        byte[] predicates,
        byte[] routingData,
        List<ColumnDescriptor> columnDescriptors) {}