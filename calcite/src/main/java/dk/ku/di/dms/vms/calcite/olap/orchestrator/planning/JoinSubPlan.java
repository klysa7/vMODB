package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.SubPlanOperation;

import java.util.List;


public record JoinSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        SubPlanOperation operation,
        List<String> columnsInOrder,
        byte[] predicates,
        byte[] routingData,
        List<ColumnDescriptor> columnDescriptors) {}