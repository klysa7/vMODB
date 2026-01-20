package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorHashJoinOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.NewCoordinatorOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.CoordinatorProjectOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;

import java.util.HashMap;
import java.util.Map;

public final class DistributedExecutor {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedExecutor.class.getName());

    private final VmsHttpClient http;
    private final CoordinatorHashJoin hashJoin;
    private final CoordinatorProject project;

    public DistributedExecutor(VmsHttpClient http) {
        this.http = http;
        this.hashJoin = new CoordinatorHashJoin();
        this.project = new CoordinatorProject();
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {

        Map<String, PushdownResponse> byExchange = new HashMap<>();

        for (VmsSubplan sp : distributedPlan.subPlans) {

            if (!(sp.operation instanceof ScanAllOperation)) {
                throw new IllegalArgumentException("we support only scan yet: " + sp.operation.getClass().getName());
            }

            PushdownResponse resp = http.executeScanAll(sp, distributedPlan.snapshot);

            byExchange.put(sp.exchangeId, resp);
        }

        if (distributedPlan.join == null) {
            throw new IllegalStateException("Plan missing join op");
        }

        CoordinatorHashJoinOperation joinOperator = distributedPlan.join;

        PushdownResponse left = byExchange.get(joinOperator.leftExchangeId);
        PushdownResponse right = byExchange.get(joinOperator.rightExchangeId);

        if (left == null || right == null) {
            throw new IllegalStateException("Missing gathered results for join");
        }

        PushdownResponse joined = hashJoin.join(left, right, joinOperator.leftKeyIndex, joinOperator.rightKeyIndex);

        byExchange.put(joinOperator.outExchangeId, joined);

        PushdownResponse result = evalNewCoordinatorOp(distributedPlan.root, byExchange);
        return result;
    }

    private PushdownResponse evalNewCoordinatorOp(NewCoordinatorOperation op, Map<String, PushdownResponse> byExchange) {
        if (op instanceof CoordinatorProjectOperation p) {

            PushdownResponse in = byExchange.get(p.inputExchangeId);
            if (in == null) throw new IllegalStateException("Missing input exchange: " + p.inputExchangeId);

            PushdownResponse result = project.project(in, p.projectedIndices);
            byExchange.put(p.outExchangeId, result);
            return result;
        }

        if (op instanceof CoordinatorHashJoinOperation j) {

            PushdownResponse left = byExchange.get(j.leftExchangeId);
            PushdownResponse right = byExchange.get(j.rightExchangeId);

            if (left == null || right == null)
                throw new IllegalStateException("Missing join inputs: " + j.leftExchangeId + ", " + j.rightExchangeId);

            PushdownResponse result = hashJoin.join(left, right, j.leftKeyIndex, j.rightKeyIndex);
            byExchange.put(j.outExchangeId, result);
            return result;
        }

        throw new IllegalArgumentException("Unknown CoordinatorOp: ");
    }
}