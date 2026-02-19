package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER = System.getLogger(DistributedExecutor.class.getName());

    private final VmsGatewayClient gatewayClient;

    public DistributedExecutor(VmsGatewayClient gatewayClient) {
        this.gatewayClient = gatewayClient;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long start = System.currentTimeMillis();
        LOGGER.log(INFO, ">>> [EXECUTOR] Starting Execution for Snapshot #" + distributedPlan.snapshot);

        CoordinatorOperator root = buildOperatorTree(distributedPlan.root, distributedPlan);

        LOGGER.log(INFO, "--> [EXECUTOR] Opening Pipeline...");
        root.open();

        List<List<Object>> allRows = new ArrayList<>();
        List<Object[]> batch;
        long totalRows = 0;

        LOGGER.log(INFO, "--> [EXECUTOR] Entering Fetch Loop...");
        try {
            while ((batch = root.nextBatch()) != null) {
                totalRows += batch.size();
                for (Object[] row : batch) {
                    allRows.add(Arrays.asList(row));
                }
            }
        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Error during execution execution", e);
            throw e;
        } finally {
            LOGGER.log(INFO, ">>> [EXECUTOR] Closing Pipeline");
            root.close();
        }

        long duration = System.currentTimeMillis() - start;
        LOGGER.log(INFO, "[EXECUTOR] FINISHED. Total Rows: " + totalRows + " Time: " + duration + "ms");

        return new PushdownResponse("gateway", distributedPlan.snapshot, null, allRows);
    }

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition def, DistributedPlan plan) {

        if (def instanceof ScanDefinition scanDef) {
            VmsSubplan subplan = plan.subPlans.stream()
                    .filter(s -> s.exchangeId.equals(scanDef.exchangeId()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Subplan not found for exchange: " + scanDef.exchangeId()));

            return new StreamingScanOperator(gatewayClient, subplan, plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            return new LocalJoinOperator(
                    buildOperatorTree(joinDef.left(), plan),
                    buildOperatorTree(joinDef.right(), plan),
                    // CHANGED: passing arrays (int[]) instead of single ints
                    joinDef.leftKeys(),
                    joinDef.rightKeys()
            );
        }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(
                    buildOperatorTree(projDef.input(), plan),
                    projDef.projectedIndices()
            );
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }
}