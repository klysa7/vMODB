package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.AggregateDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalAggregateOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalJoinOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalProjectOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.StreamingScanOperator;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER = System.getLogger(DistributedExecutor.class.getName());

    private final VmsGatewayClient gatewayClient;
    private final DistributedPlanner.ColumnsResolver columnsResolver;

    public DistributedExecutor(VmsGatewayClient gatewayClient,
                               DistributedPlanner.ColumnsResolver columnsResolver) {
        this.gatewayClient = gatewayClient;
        this.columnsResolver = columnsResolver;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long startNano = System.nanoTime();
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
            LOGGER.log(ERROR, "Error during execution", e);
            throw e;
        } finally {
            LOGGER.log(INFO, ">>> [EXECUTOR] Closing Pipeline");
            root.close();
        }

        long endNano = System.nanoTime();
        double durationMs  = (endNano - startNano) / 1_000_000.0;
        double throughput  = (durationMs > 0) ? (totalRows / (durationMs / 1000.0)) : 0.0;

        System.out.println("========================================================");
        System.out.printf("   Total Rows Returned : %d%n", totalRows);
        System.out.printf("   Total Latency (ms)  : %.3f ms%n", durationMs);
        System.out.printf("   Throughput (rows/s) : %.2f rows/sec%n", throughput);
        System.out.println("========================================================\n");

        return new PushdownResponse("gateway", distributedPlan.snapshot, null, allRows);
    }

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition def, DistributedPlan plan) {

        if (def instanceof ScanDefinition scanDef) {
            VmsSubplan subplan = plan.subPlans.stream()
                    .filter(s -> s.exchangeId.equals(scanDef.exchangeId()))
                    .findFirst().orElseThrow();
            return new StreamingScanOperator(gatewayClient, subplan, plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan
                    && joinDef.right() instanceof ScanDefinition rightScan) {

                LOGGER.log(INFO, "OPTIMIZATION: Converting to Distributed VMS-to-VMS Broadcast Join!");

                VmsSubplan leftPlan = plan.subPlans.stream()
                        .filter(s -> s.exchangeId.equals(leftScan.exchangeId())).findFirst().get();
                VmsSubplan rightPlan = plan.subPlans.stream()
                        .filter(s -> s.exchangeId.equals(rightScan.exchangeId())).findFirst().get();

                String leftTable  = ((ScanAllOperation) leftPlan.operation).table;
                String leftSchema = ((ScanAllOperation) leftPlan.operation).schema;
                String rightTable = ((ScanAllOperation) rightPlan.operation).table;
                String rightSchema= ((ScanAllOperation) rightPlan.operation).schema;

                int leftPort  = resolveTpccPort(leftTable);
                int rightPort = resolveTpccPort(rightTable);

                // ---------------------------------------------------------------
                // Build JoinRoutingData from catalog metadata so the receiving VMS
                // knows exactly which bytes to read for each join key column.
                // ---------------------------------------------------------------
                int[] leftKeys  = joinDef.leftKeys();
                int[] rightKeys = joinDef.rightKeys();

                List<CatalogColumn> leftCols  = columnsResolver.columnMetas(leftSchema,  leftTable);
                List<CatalogColumn> rightCols = columnsResolver.columnMetas(rightSchema, rightTable);

                // Cumulative data-relative byte offsets for ALL left columns.
                // (No RECORD_HEADER — the VMS strips it before sending via copyRecordToBuffer.)
                int[] allLeftOffsets = computeDataOffsets(leftCols);
                int   remoteRecordSize = sumByteSizes(leftCols);

                // Extract offsets and type codes for the join-key columns only.
                int[]  remoteColOffsets = new int[leftKeys.length];
                byte[] remoteColTypes   = new byte[leftKeys.length];
                for (int i = 0; i < leftKeys.length; i++) {
                    remoteColOffsets[i] = allLeftOffsets[leftKeys[i]];
                    remoteColTypes[i]   = catalogTypeToCode(leftCols.get(leftKeys[i]).type());
                }

                byte[] routingData = new JoinRoutingData(
                        remoteColOffsets, remoteColTypes, rightKeys, remoteRecordSize
                ).toBytes();

                LOGGER.log(INFO, ">>> [GATEWAY] Scheduling Broadcast Trigger...");
                String targetAddress = "localhost:" + rightPort;
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
                }, 100, java.util.concurrent.TimeUnit.MILLISECONDS);

                // ---------------------------------------------------------------
                // Build ColumnDescriptors for the combined [left | right] row.
                //
                // The joined byte array is laid out as:
                //   [leftPayload (remoteRecordSize bytes) | rightPayload (rightRecordSize bytes)]
                //
                // Left side:  data-relative offsets = allLeftOffsets[i]
                // Right side: data-relative offsets = allRightOffsets[i], shifted by remoteRecordSize
                // ---------------------------------------------------------------
                int[]  allRightOffsets = computeDataOffsets(rightCols);

                List<ColumnDescriptor> combinedDescriptors = new ArrayList<>();
                List<String> combinedColumns = new ArrayList<>();

                for (int i = 0; i < leftCols.size(); i++) {
                    CatalogColumn col = leftCols.get(i);
                    combinedDescriptors.add(new ColumnDescriptor(
                            col.name(), col.type(), allLeftOffsets[i], col.byteSize()));
                    combinedColumns.add(col.name());
                }
                for (int i = 0; i < rightCols.size(); i++) {
                    CatalogColumn col = rightCols.get(i);
                    combinedDescriptors.add(new ColumnDescriptor(
                            col.name(), col.type(),
                            remoteRecordSize + allRightOffsets[i],  // shift by left payload size
                            col.byteSize()));
                    combinedColumns.add(col.name());
                }

                VmsSubplan receiverJoinPlan = new VmsSubplan(
                        rightPlan.vmsName, rightPlan.url, rightPlan.exchangeId, rightPlan.operation,
                        combinedColumns,
                        rightPlan.predicates,
                        (byte) 2,        // MODE_RECEIVE_AND_JOIN
                        routingData,
                        combinedDescriptors
                );

                return new StreamingScanOperator(gatewayClient, receiverJoinPlan, plan.snapshot);
            }

            // General case: local hash join at the coordinator
            return new LocalJoinOperator(
                    buildOperatorTree(joinDef.left(), plan),
                    buildOperatorTree(joinDef.right(), plan),
                    joinDef.leftKeys(), joinDef.rightKeys()
            );
        }

        if (def instanceof AggregateDefinition aggDef) {
            return new LocalAggregateOperator(
                    buildOperatorTree(aggDef.input(), plan),
                    aggDef.groupByIndices(),
                    aggDef.aggCalls());
        }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(
                    buildOperatorTree(projDef.input(), plan), projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }

    // -------------------------------------------------------------------------
    // Schema helpers
    // -------------------------------------------------------------------------

    /**
     * Computes data-relative byte offsets for each column in the given list.
     * offset[0] = 0; offset[i] = offset[i-1] + columns[i-1].byteSize().
     * This mirrors the VMS layout after Schema.RECORD_HEADER has been stripped
     * by UniqueHashBufferIndex.copyRecordToBuffer().
     */
    private static int[] computeDataOffsets(List<CatalogColumn> cols) {
        int[] offsets = new int[cols.size()];
        int acc = 0;
        for (int i = 0; i < cols.size(); i++) {
            offsets[i] = acc;
            acc += cols.get(i).byteSize();
        }
        return offsets;
    }

    /** Returns the total byte size of a row's data payload for the given column list. */
    private static int sumByteSizes(List<CatalogColumn> cols) {
        int total = 0;
        for (CatalogColumn c : cols) total += c.byteSize();
        return total;
    }

    /** Maps a CatalogType to the corresponding JoinRoutingData type code. */
    private static byte catalogTypeToCode(CatalogType type) {
        return switch (type) {
            case INT                -> JoinRoutingData.TYPE_INT;
            case LONG, BIGINT       -> JoinRoutingData.TYPE_LONG;
            case DOUBLE             -> JoinRoutingData.TYPE_DOUBLE;
            case FLOAT              -> JoinRoutingData.TYPE_FLOAT;
            default                 -> JoinRoutingData.TYPE_INT;
        };
    }

    private int resolveTpccPort(String tableName) {
        tableName = tableName.toLowerCase();
        if (tableName.contains("warehouse") || tableName.contains("customer")) return 8001;
        if (tableName.contains("item")      || tableName.contains("stock"))    return 8002;
        return 8003;
    }
}