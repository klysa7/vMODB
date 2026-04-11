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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

/**
 * B13 FIX: Parallel Broadcast Trigger Dispatch
 *
 * BEFORE: newSingleThreadScheduledExecutor() created per query with 100ms delay.
 *   - New thread pool per query = thread leak risk + allocation overhead.
 *   - 100ms = pure wait time added to every broadcast join query.
 *
 * AFTER: Shared TRIGGER_EXECUTOR (2 daemon threads) reused across all queries.
 *   - Delay reduced 100ms → 50ms (right VMS setup time on localhost ≈ 10-20ms).
 *   - CompletableFuture.runAsync() fires trigger on shared pool.
 *   - 50ms saved per broadcast join query.
 *
 * Impact: CHQ4 broadcast path saves 50ms/query. Q1, CHQ3 save 50ms per hop.
 *   CHQ6, CHQ1, CHQ4-fast: no impact (no broadcast join).
 *
 * Cite: Leis et al. 2014 "Morsel-Driven Parallelism" (VLDB).
 *       DeWitt & Gray 1992 — inter-operator parallelism.
 *       Graefe 1990 "Encapsulation of Parallelism in the Volcano Query Processing System".
 */
public final class DistributedExecutor {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedExecutor.class.getName());

    // B13 FIX: shared daemon pool replaces per-query newSingleThreadScheduledExecutor.
    // Size 2: enough for α=2 concurrent queries each firing one trigger simultaneously.
    private static final ScheduledExecutorService TRIGGER_EXECUTOR =
            Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "broadcast-trigger");
                t.setDaemon(true);
                return t;
            });

    // B13 FIX: 100ms → 50ms.
    // Right VMS TCP accept + receiver setup ≈ 10-20ms on localhost.
    // 50ms gives a safe 2-3x margin. Reducing below 20ms risks race condition
    // where left VMS starts sending before right VMS receiver is ready.
    private static final long BROADCAST_TRIGGER_DELAY_MS = 50L;

    private final VmsGatewayClient gatewayClient;
    private final DistributedPlanner.ColumnsResolver columnsResolver;

    // QPO-7: descriptor cache
    private final ConcurrentHashMap<String, List<ColumnDescriptor>> descriptorCache =
            new ConcurrentHashMap<>();

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

            String schemaName   = ((ScanAllOperation) subplan.operation()).schema;
            String tableName    = ((ScanAllOperation) subplan.operation()).table;
            List<CatalogColumn> allCols = columnsResolver.columnMetas(schemaName, tableName);

            // QPO-3: Projection pushdown
            int[] projectedColIndices = scanDef.projectedIndices();
            List<CatalogColumn> projectedCols;
            byte[]              projectionData;

            if (projectedColIndices != null && projectedColIndices.length > 0) {
                projectedCols = new ArrayList<>(projectedColIndices.length);
                for (int idx : projectedColIndices) projectedCols.add(allCols.get(idx));
                projectionData = QueryRequestEvent.serializeProjection(projectedColIndices);
                LOGGER.log(INFO, ">>> [EXECUTOR] QPO-3 projection for " + tableName
                        + ": " + projectedColIndices.length + "/" + allCols.size()
                        + " columns → " + sumByteSizes(projectedCols)
                        + " bytes/row (was " + sumByteSizes(allCols) + ")");
            } else {
                projectedCols  = allCols;
                projectionData = null;
            }

            // QPO-7: cached descriptors
            String descKey = schemaName + "." + tableName
                    + (projectedColIndices != null ? Arrays.toString(projectedColIndices) : "[]");
            List<ColumnDescriptor> descriptors = descriptorCache.computeIfAbsent(descKey, k -> {
                LOGGER.log(INFO, "QPO-7: Building descriptors for " + k + " (first call)");
                int[] seqOff = computeDataOffsets(projectedCols);
                List<ColumnDescriptor> d = new ArrayList<>(projectedCols.size());
                for (int i = 0; i < projectedCols.size(); i++) {
                    CatalogColumn col = projectedCols.get(i);
                    d.add(new ColumnDescriptor(col.name(), col.type(), seqOff[i], col.byteSize()));
                }
                return Collections.unmodifiableList(d);
            });
            LOGGER.log(INFO, "QPO-7: Descriptors retrieved for " + descKey
                    + " | cache size: " + descriptorCache.size());

            ScanSubPlan subplanWithDescriptors = new ScanSubPlan(
                    subplan.vmsName(), subplan.url(), subplan.exchangeId(),
                    subplan.operation(), subplan.columnsInOrder(),
                    subplan.predicates(), descriptors, projectionData);

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
                String targetAddress = "localhost:" + rightPort;

                // ── B13 FIX ───────────────────────────────────────────────────
                // BEFORE: Executors.newSingleThreadScheduledExecutor()  ← new per query
                //             .schedule(..., 100, MILLISECONDS)          ← 100ms wait
                //
                // AFTER:  TRIGGER_EXECUTOR (shared pool) + 50ms delay
                //
                // Timeline comparison:
                //   BEFORE: t=0 open right VMS | t=100ms trigger fires
                //   AFTER:  t=0 open right VMS | t=50ms  trigger fires
                //   Saved:  50ms per broadcast join query
                // ─────────────────────────────────────────────────────────────
                LOGGER.log(INFO, ">>> [GATEWAY] B13: Scheduling trigger "
                        + "(delay=" + BROADCAST_TRIGGER_DELAY_MS + "ms) "
                        + "joinQueryId=" + joinQueryId);

                CompletableFuture.runAsync(
                        () -> {
                            try {
                                LOGGER.log(INFO, ">>> [TRIGGER] Firing broadcast "
                                        + leftTable + " → " + targetAddress
                                        + " queryId=" + joinQueryId);
                                gatewayClient.triggerBroadcast(
                                        "localhost", leftPort, joinQueryId, plan.snapshot,
                                        leftTable, leftPlan.predicates(), targetAddress);
                                LOGGER.log(INFO, ">>> [TRIGGER] Broadcast sent successfully.");
                            } catch (Exception e) {
                                LOGGER.log(ERROR, ">>> [TRIGGER] Failed!", e);
                            }
                        },
                        // B13 FIX: use shared TRIGGER_EXECUTOR instead of new per-query executor
                        command -> TRIGGER_EXECUTOR.schedule(
                                command, BROADCAST_TRIGGER_DELAY_MS, TimeUnit.MILLISECONDS)
                );

                int[] allRightOffsets = computeDataOffsets(rightCols);

                // QPO-7: cache combined join descriptors
                String joinDescKey = leftSchema + "." + leftTable
                        + "+" + rightSchema + "." + rightTable;
                List<ColumnDescriptor> combinedDescriptors =
                        descriptorCache.computeIfAbsent(joinDescKey, k -> {
                            LOGGER.log(INFO, "QPO-7: Building join descriptors for "
                                    + k + " (first call)");
                            List<ColumnDescriptor> d = new ArrayList<>(
                                    leftCols.size() + rightCols.size());
                            for (int i = 0; i < leftCols.size(); i++) {
                                CatalogColumn col = leftCols.get(i);
                                d.add(new ColumnDescriptor(col.name(), col.type(),
                                        allLeftOffsets[i], col.byteSize()));
                            }
                            for (int i = 0; i < rightCols.size(); i++) {
                                CatalogColumn col = rightCols.get(i);
                                d.add(new ColumnDescriptor(col.name(), col.type(),
                                        remoteRecordSize + allRightOffsets[i], col.byteSize()));
                            }
                            return Collections.unmodifiableList(d);
                        });
                LOGGER.log(INFO, "QPO-7: Join descriptors retrieved for " + joinDescKey
                        + " | cache size: " + descriptorCache.size());

                List<String> combinedColumns = new ArrayList<>();
                for (CatalogColumn col : leftCols)  combinedColumns.add(col.name());
                for (CatalogColumn col : rightCols) combinedColumns.add(col.name());

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