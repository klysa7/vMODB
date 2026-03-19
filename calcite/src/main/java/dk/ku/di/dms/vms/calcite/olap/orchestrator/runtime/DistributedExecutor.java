package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.JoinSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ScanSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.AggregateDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalAggregateOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalJoinOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalProjectOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.StreamingScanOperator;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;

import java.net.URI;
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
        root.open();

        List<List<Object>> allRows = new ArrayList<>();
        List<Object[]> batch;
        long totalRows = 0;

        try {
            while ((batch = root.nextBatch()) != null) {
                totalRows += batch.size();
                for (Object[] row : batch) allRows.add(Arrays.asList(row));
            }
        } catch (Exception e) {
            LOGGER.log(ERROR, "Error during execution", e);
            throw e;
        } finally {
            LOGGER.log(INFO, ">>> [EXECUTOR] Closing Pipeline");
            root.close();
        }

        long endNano = System.nanoTime();
        double durationMs = (endNano - startNano) / 1_000_000.0;
        System.out.printf("========================================================%n");
        System.out.printf("   Total Rows Returned : %d%n", totalRows);
        System.out.printf("   Total Latency (ms)  : %.3f ms%n", durationMs);
        System.out.printf("   Throughput (rows/s) : %.2f rows/sec%n",
                durationMs > 0 ? totalRows / (durationMs / 1000.0) : 0.0);
        System.out.printf("========================================================%n%n");

        return new PushdownResponse("gateway", distributedPlan.snapshot, null, allRows);
    }

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition def, DistributedPlan plan) {

        if (def instanceof ScanDefinition scanDef) {
            // A7: find the typed subplan — it's a ScanSubplan for plain scans
            ScanSubPlan subplan = plan.subPlans.stream()
                    .filter(s -> s instanceof ScanSubPlan ss && ss.exchangeId().equals(scanDef.exchangeId()))
                    .map(s -> (ScanSubPlan) s)
                    .findFirst().orElseThrow();
            return new StreamingScanOperator(gatewayClient, subplan, plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan
                    && joinDef.right() instanceof ScanDefinition rightScan) {

                LOGGER.log(INFO, "OPTIMIZATION: Converting to Distributed VMS-to-VMS Broadcast Join!");

                // A7: both sides start as ScanSubplans from the planner
                ScanSubPlan leftPlan = plan.subPlans.stream()
                        .filter(s -> s instanceof ScanSubPlan ss && ss.exchangeId().equals(leftScan.exchangeId()))
                        .map(s -> (ScanSubPlan) s).findFirst().get();
                ScanSubPlan rightPlan = plan.subPlans.stream()
                        .filter(s -> s instanceof ScanSubPlan ss && ss.exchangeId().equals(rightScan.exchangeId()))
                        .map(s -> (ScanSubPlan) s).findFirst().get();

                String leftTable  = ((ScanAllOperation) leftPlan.operation()).table;
                String leftSchema = ((ScanAllOperation) leftPlan.operation()).schema;
                String rightTable = ((ScanAllOperation) rightPlan.operation()).table;
                String rightSchema= ((ScanAllOperation) rightPlan.operation()).schema;

                // A1+A7: parse ports from URL, no hardcoded maps
                int leftPort  = URI.create(leftPlan.url()).getPort();
                int rightPort = URI.create(rightPlan.url()).getPort();

                int[] leftKeys  = joinDef.leftKeys();
                int[] rightKeys = joinDef.rightKeys();

                List<CatalogColumn> leftCols  = columnsResolver.columnMetas(leftSchema, leftTable);
                List<CatalogColumn> rightCols = columnsResolver.columnMetas(rightSchema, rightTable);

                int[] allLeftOffsets   = computeDataOffsets(leftCols);
                int   remoteRecordSize = sumByteSizes(leftCols);

                int[]  remoteColOffsets = new int[leftKeys.length];
                byte[] remoteColTypes   = new byte[leftKeys.length];
                for (int i = 0; i < leftKeys.length; i++) {
                    remoteColOffsets[i] = allLeftOffsets[leftKeys[i]];
                    remoteColTypes[i]   = catalogTypeToCode(leftCols.get(leftKeys[i]).type());
                }

                byte[] routingData = new JoinRoutingData(
                        remoteColOffsets, remoteColTypes, rightKeys, remoteRecordSize).toBytes();

                long joinQueryId = StreamingScanOperator.nextQueryId();

                LOGGER.log(INFO, ">>> [GATEWAY] Scheduling Broadcast Trigger... joinQueryId=" + joinQueryId);
                String targetAddress = "localhost:" + rightPort;
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor().schedule(() -> {
                    try {
                        LOGGER.log(INFO, ">>> [TRIGGER THREAD] Firing Broadcast from " + leftTable
                                + " to " + targetAddress + " queryId=" + joinQueryId);
                        gatewayClient.triggerBroadcast(
                                "localhost", leftPort, joinQueryId, plan.snapshot,
                                leftTable, leftPlan.predicates(), targetAddress);
                        LOGGER.log(INFO, ">>> [TRIGGER THREAD] Successfully signaled Warehouse VMS.");
                    } catch (Exception e) {
                        LOGGER.log(ERROR, ">>> [TRIGGER THREAD] Failed to signal Warehouse!", e);
                    }
                }, 100, java.util.concurrent.TimeUnit.MILLISECONDS);

                int[] allRightOffsets = computeDataOffsets(rightCols);

                List<ColumnDescriptor> combinedDescriptors = new ArrayList<>();
                List<String> combinedColumns = new ArrayList<>();

                for (int i = 0; i < leftCols.size(); i++) {
                    CatalogColumn col = leftCols.get(i);
                    combinedDescriptors.add(new ColumnDescriptor(col.name(), col.type(), allLeftOffsets[i], col.byteSize()));
                    combinedColumns.add(col.name());
                }
                for (int i = 0; i < rightCols.size(); i++) {
                    CatalogColumn col = rightCols.get(i);
                    combinedDescriptors.add(new ColumnDescriptor(col.name(), col.type(), remoteRecordSize + allRightOffsets[i], col.byteSize()));
                    combinedColumns.add(col.name());
                }

                // A7: the probe side becomes a JoinSubplan — its role is now
                // explicit. No mode byte, no null check on columnDescriptors.
                JoinSubPlan receiverJoinPlan = new JoinSubPlan(
                        rightPlan.vmsName(),
                        rightPlan.url(),
                        rightPlan.exchangeId(),
                        rightPlan.operation(),
                        combinedColumns,
                        rightPlan.predicates(),
                        routingData,
                        combinedDescriptors
                );

                return new StreamingScanOperator(gatewayClient, receiverJoinPlan, plan.snapshot, joinQueryId);
            }

            return new LocalJoinOperator(
                    buildOperatorTree(joinDef.left(), plan),
                    buildOperatorTree(joinDef.right(), plan),
                    joinDef.leftKeys(), joinDef.rightKeys());
        }

        if (def instanceof AggregateDefinition aggDef) {
            return new LocalAggregateOperator(
                    buildOperatorTree(aggDef.input(), plan),
                    aggDef.groupByIndices(), aggDef.aggCalls());
        }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(
                    buildOperatorTree(projDef.input(), plan), projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }

    private static int[] computeDataOffsets(List<CatalogColumn> cols) {
        int[] offsets = new int[cols.size()];
        int acc = 0;
        for (int i = 0; i < cols.size(); i++) { offsets[i] = acc; acc += cols.get(i).byteSize(); }
        return offsets;
    }

    private static int sumByteSizes(List<CatalogColumn> cols) {
        int total = 0;
        for (CatalogColumn c : cols) total += c.byteSize();
        return total;
    }

    private static byte catalogTypeToCode(CatalogType type) {
        return switch (type) {
            case INT            -> JoinRoutingData.TYPE_INT;
            case LONG, BIGINT   -> JoinRoutingData.TYPE_LONG;
            case DOUBLE         -> JoinRoutingData.TYPE_DOUBLE;
            case FLOAT          -> JoinRoutingData.TYPE_FLOAT;
            default             -> JoinRoutingData.TYPE_INT;
        };
    }
}