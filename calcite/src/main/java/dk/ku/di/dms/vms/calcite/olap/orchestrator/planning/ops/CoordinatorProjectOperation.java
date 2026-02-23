package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public final class CoordinatorProjectOperation implements CoordinatorOperation {
    public final String inputExchangeId;
    public final int[] projectedIndices;
    public final String outExchangeId;

    public CoordinatorProjectOperation(String inputExchangeId, int[] projectedIndices, String outExchangeId) {
        this.inputExchangeId = inputExchangeId;
        this.projectedIndices = projectedIndices;
        this.outExchangeId = outExchangeId;
    }
}