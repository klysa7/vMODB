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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;
/**
 * Translates a {@link DistributedPlan} into a Volcano-style operator tree
 * and drives its execution to completion. Scan leaves become
 * {@link StreamingScanOperator} instances that pull rows from remote VMSes
 * via {@link VmsGatewayClient}; join nodes are either converted to a
 * distributed broadcast join (one VMS streams its scan
 * directly to another) or fall back to a local hash join at the gateway.
 * Projection and aggregation nodes are always executed locally.
 * Column-descriptor and join-descriptor lists are cached across calls
 * and cache hit/miss statistics are dumped to stderr on shutdown.
 */
public final class DistributedExecutor {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedExecutor.class.getName());
    private static final ScheduledExecutorService TRIGGER_EXECUTOR =
            Executors.newScheduledThreadPool(2, r -> {
                Thread t = new Thread(r, "broadcast-trigger");
                t.setDaemon(true);
                return t;
            });

    private static final long BROADCAST_TRIGGER_DELAY_MS = 50L;
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            TRIGGER_EXECUTOR.shutdown();
            try { TRIGGER_EXECUTOR.awaitTermination(3, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { }
        }, "trigger-executor-shutdown"));
    }

    private final VmsGatewayClient gatewayClient;
    private final DistributedPlanner.ColumnsResolver columnsResolver;
    private final ConcurrentHashMap<String, List<ColumnDescriptor>> descriptorCache =
            new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, CacheStats> CACHE_STATS =
            new ConcurrentHashMap<>();
    private static final TriggerStats TRIGGER_STATS = new TriggerStats();
    private static final class TriggerStats {
        final AtomicLong triggersScheduled     = new AtomicLong();
        final AtomicLong dispatchLatencyTotalNs = new AtomicLong();
        final AtomicLong triggerExecTotalNs    = new AtomicLong();
        final AtomicLong triggerFailures       = new AtomicLong();
    }

    static {
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            dumpCacheStats();
//            dumpTriggerStats();
//        }, "stats-dump"));
    }

    private static final class CacheStats {
        final AtomicLong missCount   = new AtomicLong();
        final AtomicLong missTotalNs = new AtomicLong();
        final AtomicLong hitCount    = new AtomicLong();
        final AtomicLong hitTotalNs  = new AtomicLong();
    }

    private static CacheStats statsFor(String key) {
        return CACHE_STATS.computeIfAbsent(key, k -> new CacheStats());
    }

    private static <V> V timedCacheLookup(String key,
                                          ConcurrentHashMap<String, V> cache,
                                          java.util.function.Function<String, V> supplier) {
        CacheStats stats = statsFor(key);
        long t0 = System.nanoTime();


        V existing = cache.get(key);
        if (existing != null) {
            stats.hitCount.incrementAndGet();
            stats.hitTotalNs.addAndGet(System.nanoTime() - t0);
            return existing;
        }

        boolean[] ranLambda = { false };
        V value = cache.computeIfAbsent(key, k -> {
            ranLambda[0] = true;
            return supplier.apply(k);
        });
        long elapsed = System.nanoTime() - t0;

        if (ranLambda[0]) {
            stats.missCount.incrementAndGet();
            stats.missTotalNs.addAndGet(elapsed);
        } else {
            stats.hitCount.incrementAndGet();
            stats.hitTotalNs.addAndGet(elapsed);
        }
        return value;
    }

    private static void dumpCacheStats() {
        if (CACHE_STATS.isEmpty()) return;
        System.err.println();
        System.err.println("========================================================");
        System.err.println("Cache Stats (per-key)");
        System.err.println("========================================================");
        System.err.printf("%-60s %8s %14s %8s %14s %10s%n",
                "key", "misses", "first_ns_avg", "hits", "hit_ns_avg", "savings_ms");
        System.err.println("-".repeat(120));

        long totalHits = 0, totalMisses = 0, totalSavedNs = 0;

        for (Map.Entry<String, CacheStats> e : CACHE_STATS.entrySet()) {
            CacheStats s = e.getValue();
            long misses = s.missCount.get();
            long hits   = s.hitCount.get();
            long missAvg = misses > 0 ? s.missTotalNs.get() / misses : 0;
            long hitAvg  = hits   > 0 ? s.hitTotalNs.get()  / hits   : 0;
            long savedNs = hits * Math.max(0, missAvg - hitAvg);

            totalHits    += hits;
            totalMisses  += misses;
            totalSavedNs += savedNs;

            String key = e.getKey();
            if (key.length() > 58) key = key.substring(0, 55) + "...";

            System.err.printf("%-60s %8d %14d %8d %14d %10.2f%n",
                    key, misses, missAvg, hits, hitAvg, savedNs / 1_000_000.0);
        }
        System.err.println("-".repeat(120));
        System.err.printf("  TOTAL  misses=%d  hits=%d  hit_rate=%.3f  cumulative_saving=%.2f ms%n",
                totalMisses, totalHits,
                (totalHits + totalMisses) == 0 ? 0.0 :
                        (double) totalHits / (totalHits + totalMisses),
                totalSavedNs / 1_000_000.0);
        System.err.println("========================================================");
    }

    private static void dumpTriggerStats() {
        long n = TRIGGER_STATS.triggersScheduled.get();
        if (n == 0) return;
        long dispatchAvgNs = TRIGGER_STATS.dispatchLatencyTotalNs.get() / n;
        long execAvgNs     = TRIGGER_STATS.triggerExecTotalNs.get()    / n;
        long failures      = TRIGGER_STATS.triggerFailures.get();

        System.err.println();
        System.err.println("========================================================");
        System.err.printf("  configured_delay_ms      = %d%n", BROADCAST_TRIGGER_DELAY_MS);
        System.err.printf("  triggers_scheduled       = %d%n", n);
        System.err.printf("  dispatch_latency_avg     = %.2f ms   (configured + scheduler overhead)%n",
                dispatchAvgNs / 1_000_000.0);
        System.err.printf("  trigger_exec_avg         = %.2f ms   (triggerBroadcast() call time)%n",
                execAvgNs / 1_000_000.0);
        System.err.printf("  trigger_failures         = %d%n", failures);
        System.err.println("--------------------------------------------------------");
        System.err.printf("saving vs 100ms baseline: %.2f ms per trigger × %d triggers%n",
                Math.max(0.0, 100.0 - dispatchAvgNs / 1_000_000.0), n);
        System.err.printf("  Cumulative: %.2f ms%n",
                Math.max(0.0, 100.0 - dispatchAvgNs / 1_000_000.0) * n);
        System.err.println("========================================================");
    }

    public DistributedExecutor(VmsGatewayClient gatewayClient,
                               DistributedPlanner.ColumnsResolver columnsResolver) {
        this.gatewayClient   = gatewayClient;
        this.columnsResolver = columnsResolver;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long startNano = System.nanoTime();
        LOGGER.log(INFO, "Starting Execution for Snapshot #"
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

        if (def instanceof ScanDefinition scanDef) {
            ScanSubPlan subplan = plan.subPlans.stream()
                    .filter(s -> s instanceof ScanSubPlan ss
                            && ss.exchangeId().equals(scanDef.exchangeId()))
                    .map(s -> (ScanSubPlan) s)
                    .findFirst().orElseThrow();

            String schemaName   = ((ScanAllOperation) subplan.operation()).schema;
            String tableName    = ((ScanAllOperation) subplan.operation()).table;
            List<CatalogColumn> allCols = columnsResolver.columnMetas(schemaName, tableName);

            int[] projectedColIndices = scanDef.projectedIndices(); // may be null

            List<CatalogColumn> projectedCols;
            byte[]              projectionData;

            if (projectedColIndices != null && projectedColIndices.length > 0) {
                projectedCols = new ArrayList<>(projectedColIndices.length);
                for (int idx : projectedColIndices) {
                    projectedCols.add(allCols.get(idx));
                }
                projectionData = QueryRequestEvent.serializeProjection(projectedColIndices);

//                LOGGER.log(INFO, "projection for " + tableName
//                        + ": " + projectedColIndices.length + "/" + allCols.size()
//                        + " columns → "
//                        + sumByteSizes(projectedCols) + " bytes/row (was "
//                        + sumByteSizes(allCols) + ")");
            } else {
                projectedCols  = allCols;
                projectionData = null;
            }

            final List<CatalogColumn> projectedColsFinal = projectedCols;
            String descKey = "scan:" + schemaName + "." + tableName
                    + (projectedColIndices != null ? Arrays.toString(projectedColIndices) : "[]");

            List<ColumnDescriptor> descriptors = timedCacheLookup(descKey, descriptorCache, k -> {
//                LOGGER.log(INFO, "Building descriptors for " + k + " (first call)");
                int[] seqOff = computeDataOffsets(projectedColsFinal);
                List<ColumnDescriptor> d = new ArrayList<>(projectedColsFinal.size());
                for (int i = 0; i < projectedColsFinal.size(); i++) {
                    CatalogColumn col = projectedColsFinal.get(i);
                    d.add(new ColumnDescriptor(col.name(), col.type(), seqOff[i], col.byteSize()));
                }
                return Collections.unmodifiableList(d);
            });

            ScanSubPlan subplanWithDescriptors = new ScanSubPlan(
                    subplan.vmsName(), subplan.url(), subplan.exchangeId(),
                    subplan.operation(), subplan.columnsInOrder(),
                    subplan.predicates(), descriptors,
                    projectionData);

            return new StreamingScanOperator(gatewayClient, subplanWithDescriptors,
                    plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan
                    && joinDef.right() instanceof ScanDefinition rightScan) {

//                LOGGER.log(INFO,
//                        "OPTIMIZATION: Converting to Distributed VMS-to-VMS Broadcast Join!");

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
                final DistributedPlan planFinal = plan;
                final ScanSubPlan leftPlanFinal = leftPlan;

//                LOGGER.log(INFO, "Scheduling trigger "
//                        + "(delay=" + BROADCAST_TRIGGER_DELAY_MS + "ms) "
//                        + "joinQueryId=" + joinQueryId);

                final long scheduleTimeNs = System.nanoTime();
                TRIGGER_STATS.triggersScheduled.incrementAndGet();

                CompletableFuture.runAsync(
                        () -> {
                            long fireTimeNs = System.nanoTime();
                            TRIGGER_STATS.dispatchLatencyTotalNs.addAndGet(fireTimeNs - scheduleTimeNs);
                            try {
                                LOGGER.log(INFO, "Firing broadcast "
                                        + leftTable + " → " + targetAddress
                                        + " queryId=" + joinQueryId
                                        + " (dispatch_latency="
                                        + ((fireTimeNs - scheduleTimeNs) / 1_000_000) + "ms)");
                                gatewayClient.triggerBroadcast(
                                        "localhost", leftPort, joinQueryId, planFinal.snapshot,
                                        leftTable, leftPlanFinal.predicates(), targetAddress);
                                long execEndNs = System.nanoTime();
                                TRIGGER_STATS.triggerExecTotalNs.addAndGet(execEndNs - fireTimeNs);
//                                LOGGER.log(INFO, "Broadcast sent successfully "
//                                        + "(exec_time=" + ((execEndNs - fireTimeNs) / 1_000_000) + "ms)");
                            } catch (Exception e) {
                                TRIGGER_STATS.triggerFailures.incrementAndGet();
                                LOGGER.log(ERROR, "Failed!", e);
                            }
                        },
                        command -> TRIGGER_EXECUTOR.schedule(
                                command, BROADCAST_TRIGGER_DELAY_MS, TimeUnit.MILLISECONDS)
                );

                int[] allRightOffsets = computeDataOffsets(rightCols);

                final List<CatalogColumn> leftColsFinal  = leftCols;
                final List<CatalogColumn> rightColsFinal = rightCols;
                final int[] allLeftOffsetsFinal  = allLeftOffsets;
                final int[] allRightOffsetsFinal = allRightOffsets;
                final int   remoteRecordSizeFinal = remoteRecordSize;

                String joinDescKey = "join:" + leftSchema + "." + leftTable
                        + "+" + rightSchema + "." + rightTable;

                List<ColumnDescriptor> combinedDescriptors =
                        timedCacheLookup(joinDescKey, descriptorCache, k -> {
//                            LOGGER.log(INFO, "Building join descriptors for " + k + " (first call)");
                            List<ColumnDescriptor> d = new ArrayList<>(
                                    leftColsFinal.size() + rightColsFinal.size());
                            for (int i = 0; i < leftColsFinal.size(); i++) {
                                CatalogColumn col = leftColsFinal.get(i);
                                d.add(new ColumnDescriptor(col.name(), col.type(),
                                        allLeftOffsetsFinal[i], col.byteSize()));
                            }
                            for (int i = 0; i < rightColsFinal.size(); i++) {
                                CatalogColumn col = rightColsFinal.get(i);
                                d.add(new ColumnDescriptor(col.name(), col.type(),
                                        remoteRecordSizeFinal + allRightOffsetsFinal[i],
                                        col.byteSize()));
                            }
                            return Collections.unmodifiableList(d);
                        });

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

        if (def instanceof AggregateDefinition aggDef) {
            return new LocalAggregateOperator(
                    buildOperatorTree(aggDef.input(), plan),
                    aggDef.groupByIndices(), aggDef.aggCalls());
        }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(
                    buildOperatorTree(projDef.input(), plan),
                    projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }


    private static int[] computeDataOffsets(List<CatalogColumn> columns) {
        int[] offsets = new int[columns.size()];
        int acc = 0;
        for (int i = 0; i < columns.size(); i++) {
            offsets[i] = acc;
            acc += columns.get(i).byteSize();
        }
        return offsets;
    }

    private static int sumByteSizes(List<CatalogColumn> columns) {
        int total = 0;
        for (CatalogColumn c : columns) total += c.byteSize();
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