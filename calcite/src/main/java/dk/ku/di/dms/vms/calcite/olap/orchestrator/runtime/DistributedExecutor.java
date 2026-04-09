package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.JoinSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ScanSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalAggregateOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalJoinOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalProjectOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.StreamingScanOperator;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedExecutor.class.getName());

    private final VmsGatewayClient gatewayClient;
    private final DistributedPlanner.ColumnsResolver columnsResolver;

    public DistributedExecutor(VmsGatewayClient gatewayClient,
                               DistributedPlanner.ColumnsResolver columnsResolver) {
        this.gatewayClient   = gatewayClient;
        this.columnsResolver = columnsResolver;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long startNano = System.nanoTime();
        LOGGER.log(INFO, ">>> [EXECUTOR] Starting Execution for Snapshot #"
                + distributedPlan.snapshot);

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

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition def,
                                                  DistributedPlan plan) {

        // ── Scan ─────────────────────────────────────────────────────────────
        if (def instanceof ScanDefinition scanDef) {
            ScanSubPlan subplan = plan.subPlans.stream()
                    .filter(s -> s instanceof ScanSubPlan ss
                            && ss.exchangeId().equals(scanDef.exchangeId()))
                    .map(s -> (ScanSubPlan) s)
                    .findFirst().orElseThrow();

            String schemaName       = ((ScanAllOperation) subplan.operation()).schema;
            String tableName        = ((ScanAllOperation) subplan.operation()).table;
            List<CatalogColumn> allCols = columnsResolver.columnMetas(schemaName, tableName);

            // ── QPO-3: Projection pushdown ────────────────────────────────────
            //
            // scanDef.projectedIndices() comes from VModbTableAccess.projects,
            // which is populated by VModbTableAccessRule when Calcite's logical
            // plan has a Project on top of a TableScan (B61 fix).
            //
            // If projects is still null (B61 not yet fixed, or query selects *),
            // we fall back to all columns — fully backward compatible.
            //
            // The critical invariant: projected descriptors must use SEQUENTIAL
            // offsets (0, 4, 8, ...) matching the projected byte[] layout that
            // serializeRowProjected() on the VMS writes. These are NOT the
            // original schema offsets — only the projected columns' sizes matter.
            // VmsResultIterator.parseRowData() reads using these sequential
            // offsets correctly from the smaller byte[].
            // ──────────────────────────────────────────────────────────────────
            int[] projectedColIndices = scanDef.projectedIndices(); // may be null

            List<CatalogColumn> projectedCols;
            byte[]              projectionData;

            if (projectedColIndices != null && projectedColIndices.length > 0) {
                // Build the projected column list in the requested order
                projectedCols = new ArrayList<>(projectedColIndices.length);
                for (int idx : projectedColIndices) {
                    projectedCols.add(allCols.get(idx));
                }
                projectionData = QueryRequestEvent.serializeProjection(projectedColIndices);

                LOGGER.log(INFO, ">>> [EXECUTOR] QPO-3 projection for " + tableName
                        + ": " + projectedColIndices.length + "/" + allCols.size()
                        + " columns → "
                        + sumByteSizes(projectedCols) + " bytes/row (was "
                        + sumByteSizes(allCols) + ")");
            } else {
                // No projection — send all columns (current behavior)
                projectedCols  = allCols;
                projectionData = null;
            }

            // Sequential offsets for the output byte[] on the gateway side
            int[] seqOffsets = computeDataOffsets(projectedCols);
            List<ColumnDescriptor> descriptors = new ArrayList<>(projectedCols.size());
            for (int i = 0; i < projectedCols.size(); i++) {
                CatalogColumn col = projectedCols.get(i);
                descriptors.add(new ColumnDescriptor(
                        col.name(), col.type(),
                        seqOffsets[i],   // sequential — NOT original schema offset
                        col.byteSize()));
            }

            ScanSubPlan subplanWithDescriptors = new ScanSubPlan(
                    subplan.vmsName(), subplan.url(), subplan.exchangeId(),
                    subplan.operation(), subplan.columnsInOrder(),
                    subplan.predicates(), descriptors,
                    projectionData);    // QPO-3: pass projection to StreamingScanOperator

            return new StreamingScanOperator(gatewayClient, subplanWithDescriptors,
                    plan.snapshot);
        }

        // ── Broadcast Join ───────────────────────────────────────────────────
        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan
                    && joinDef.right() instanceof ScanDefinition rightScan) {

                LOGGER.log(INFO,
                        "OPTIMIZATION: Converting to Distributed VMS-to-VMS Broadcast Join!");

                ScanSubPlan leftPlan = plan.subPlans.stream()
                        .filter(s -> s instanceof ScanSubPlan ss
                                && ss.exchangeId().equals(leftScan.exchangeId()))
                        .map(s -> (ScanSubPlan) s).findFirst().get();
                ScanSubPlan rightPlan = plan.subPlans.stream()
                        .filter(s -> s instanceof ScanSubPlan ss
                                && ss.exchangeId().equals(rightScan.exchangeId()))
                        .map(s -> (ScanSubPlan) s).findFirst().get();

                String leftTable  = ((ScanAllOperation) leftPlan.operation()).table;
                String leftSchema = ((ScanAllOperation) leftPlan.operation()).schema;
                String rightTable = ((ScanAllOperation) rightPlan.operation()).table;
                String rightSchema= ((ScanAllOperation) rightPlan.operation()).schema;

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
                        remoteColOffsets, remoteColTypes, rightKeys,
                        remoteRecordSize).toBytes();

                long joinQueryId = StreamingScanOperator.nextQueryId();

                LOGGER.log(INFO, ">>> [GATEWAY] Scheduling Broadcast Trigger... joinQueryId="
                        + joinQueryId);
                String targetAddress = "localhost:" + rightPort;
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
                        .schedule(() -> {
                            try {
                                LOGGER.log(INFO, ">>> [TRIGGER THREAD] Firing Broadcast from "
                                        + leftTable + " to " + targetAddress
                                        + " queryId=" + joinQueryId);
                                // Note: broadcast sends FULL rows to probe VMS — no projection
                                gatewayClient.triggerBroadcast(
                                        "localhost", leftPort, joinQueryId, plan.snapshot,
                                        leftTable, leftPlan.predicates(), targetAddress);
                                LOGGER.log(INFO,
                                        ">>> [TRIGGER THREAD] Successfully signaled Warehouse VMS.");
                            } catch (Exception e) {
                                LOGGER.log(ERROR, ">>> [TRIGGER THREAD] Failed to signal Warehouse!", e);
                            }
                        }, 100, java.util.concurrent.TimeUnit.MILLISECONDS);

                int[] allRightOffsets = computeDataOffsets(rightCols);

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
                            remoteRecordSize + allRightOffsets[i], col.byteSize()));
                    combinedColumns.add(col.name());
                }

                JoinSubPlan receiverJoinPlan = new JoinSubPlan(
                        rightPlan.vmsName(), rightPlan.url(), rightPlan.exchangeId(),
                        rightPlan.operation(), combinedColumns,
                        rightPlan.predicates(), routingData, combinedDescriptors);

                return new StreamingScanOperator(gatewayClient, receiverJoinPlan,
                        plan.snapshot, joinQueryId);
            }

            return new LocalJoinOperator(
                    buildOperatorTree(joinDef.left(), plan),
                    buildOperatorTree(joinDef.right(), plan),
                    joinDef.leftKeys(), joinDef.rightKeys());
        }

        // ── Aggregate ─────────────────────────────────────────────────────────
        if (def instanceof AggregateDefinition aggDef) {
            return new LocalAggregateOperator(
                    buildOperatorTree(aggDef.input(), plan),
                    aggDef.groupByIndices(), aggDef.aggCalls());
        }

        // ── Project ───────────────────────────────────────────────────────────
        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(
                    buildOperatorTree(projDef.input(), plan),
                    projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static int[] computeDataOffsets(List<CatalogColumn> cols) {
        int[] offsets = new int[cols.size()];
        int acc = 0;
        for (int i = 0; i < cols.size(); i++) {
            offsets[i] = acc;
            acc += cols.get(i).byteSize();
        }
        return offsets;
    }

    private static int sumByteSizes(List<CatalogColumn> cols) {
        int total = 0;
        for (CatalogColumn c : cols) total += c.byteSize();
        return total;
    }

    private static byte catalogTypeToCode(CatalogType type) {
        return switch (type) {
            case INT           -> JoinRoutingData.TYPE_INT;
            case LONG, BIGINT  -> JoinRoutingData.TYPE_LONG;
            case DOUBLE        -> JoinRoutingData.TYPE_DOUBLE;
            case FLOAT         -> JoinRoutingData.TYPE_FLOAT;
            default            -> JoinRoutingData.TYPE_INT;
        };
    }
}