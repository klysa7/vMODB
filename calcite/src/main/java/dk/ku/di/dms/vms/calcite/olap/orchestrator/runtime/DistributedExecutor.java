package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.ERROR;
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
            LOGGER.log(ERROR, "Error during execution execution", e);
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
                    .orElseThrow();
            return new StreamingScanOperator(gatewayClient, subplan, plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan && joinDef.right() instanceof ScanDefinition rightScan) {
                LOGGER.log(INFO, "OPTIMIZATION: Converting to Distributed VMS-to-VMS Broadcast Join!");

                VmsSubplan leftPlan = plan.subPlans.stream().filter(s -> s.exchangeId.equals(leftScan.exchangeId())).findFirst().get();
                VmsSubplan rightPlan = plan.subPlans.stream().filter(s -> s.exchangeId.equals(rightScan.exchangeId())).findFirst().get();

                String leftTable = ((ScanAllOperation) leftPlan.operation).table;
                String rightTable = ((ScanAllOperation) rightPlan.operation).table;

                int leftPort = resolveTpccPort(leftTable);
                int rightPort = resolveTpccPort(rightTable);

                String targetAddress = "localhost:" + rightPort;
                LOGGER.log(INFO, ">>> [GATEWAY] Scheduling Broadcast Trigger...");
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                    try {
                        LOGGER.log(INFO, ">>> [TRIGGER THREAD] Firing Broadcast from " + leftTable + " to " + targetAddress);
                        gatewayClient.triggerBroadcast(
                                "localhost", leftPort, plan.snapshot, plan.snapshot,
                                leftTable, leftPlan.predicates, targetAddress
                        );
                        LOGGER.log(INFO, ">>> [TRIGGER THREAD] Successfully signaled Warehouse VMS.");
                    } catch (Exception e) {
                        LOGGER.log(ERROR, ">>> [TRIGGER THREAD] Failed to signal Warehouse!", e);
                    }
                }, 1, java.util.concurrent.TimeUnit.SECONDS); // Give it a full 1 second to be safe

                byte[] joinColumnIndexData = "3".getBytes(java.nio.charset.StandardCharsets.UTF_8);

                // =========================================================
                // 🔥 FIX 1: MERGE THE SCHEMAS FOR THE GATEWAY PARSER
                // =========================================================
                List<String> combinedColumns = new java.util.ArrayList<>(leftPlan.columnsInOrder);
                combinedColumns.addAll(rightPlan.columnsInOrder);

                VmsSubplan receiverJoinPlan = new VmsSubplan(
                        rightPlan.vmsName, rightPlan.url, rightPlan.exchangeId, rightPlan.operation,
                        combinedColumns, // <-- Pass the combined schema here!
                        rightPlan.predicates, (byte) 2, joinColumnIndexData
                );

                return new StreamingScanOperator(gatewayClient, receiverJoinPlan, plan.snapshot);
            }

            return new LocalJoinOperator(
                    buildOperatorTree(joinDef.left(), plan),
                    buildOperatorTree(joinDef.right(), plan),
                    joinDef.leftKeys(), joinDef.rightKeys()
            );
        }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(buildOperatorTree(projDef.input(), plan), projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }

    // Helper to resolve ports
    private int resolveTpccPort(String tableName) {
        tableName = tableName.toLowerCase();
        if (tableName.contains("warehouse") || tableName.contains("customer")) return 8001;
        if (tableName.contains("item") || tableName.contains("stock")) return 8002;
        return 8003;
    }
}