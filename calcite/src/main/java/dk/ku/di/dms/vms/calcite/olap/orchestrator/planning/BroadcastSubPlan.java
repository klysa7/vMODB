package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;


public record BroadcastSubPlan(
        String vmsName,
        String url,
        String exchangeId,
        byte[] predicates,
        String targetAddress) {}