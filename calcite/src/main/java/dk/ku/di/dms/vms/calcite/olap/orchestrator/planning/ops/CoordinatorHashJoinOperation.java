package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public final class CoordinatorHashJoinOperation implements NewCoordinatorOperation {
    public final String leftExchangeId;
    public final String rightExchangeId;
    public final Integer leftKeyIndex;
    public final Integer rightKeyIndex;
    public final String outExchangeId;

    public CoordinatorHashJoinOperation(String leftExchangeId, String rightExchangeId,
                                        Integer leftKeyIndex, Integer rightKeyIndex, String outExchangeId) {
        this.leftExchangeId = leftExchangeId;
        this.rightExchangeId = rightExchangeId;
        this.leftKeyIndex = leftKeyIndex;
        this.rightKeyIndex = rightKeyIndex;
        this.outExchangeId = outExchangeId;
    }
}