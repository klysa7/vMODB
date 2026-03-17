package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

/**
 * A7: Build side subplan.
 * The Warehouse VMS scans its table and streams rows directly to the probe VMS.
 * Mode 1. No columnDescriptors needed — build side just sends raw bytes,
 * it does not talk to the gateway.
 */
public record BroadcastSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        byte[] predicates,
        String targetAddress) {}