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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER =
            System.getLogger(DistributedExecutor.class.getName());

    private final VmsGatewayClient gatewayClient;
    private final DistributedPlanner.ColumnsResolver columnsResolver;

    // ── QPO-7: Descriptor cache ───────────────────────────────────────────────
    // columnsResolver.columnMetas() iterates the full catalog on every call.
    // Cache the resulting ColumnDescriptor list per schema.table — built once
    // on the first query, reused on all subsequent queries.
    // ConcurrentHashMap: safe under concurrent OLAP workers (α≥2).
    // Key: "schema.table[projection]". Value: immutable ColumnDescriptor list.
    private final ConcurrentHashMap<String, List<ColumnDescriptor>> descriptorCache =
            new ConcurrentHashMap<>();

    // ── QPO-7: Cache-hit instrumentation ──────────────────────────────────────
    // Mirrors QPO-1 PlanStats — per-key counters of misses (first-call latency),
    // hits (cached-call latency), and cumulative time savings. Dumped via
    // JVM shutdown hook so measurement runs emit the table to stderr.
    //
    // Expected shape at 60s / α=2: hits=hundreds, misses=few (≤10).
    // Headline metric: cached_avg_ns / first_call_ns — ratio is the per-query
    // setup-cost reduction QPO-7 delivers.
    private static final ConcurrentHashMap<String, CacheStats> CACHE_STATS =
            new ConcurrentHashMap<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(
                DistributedExecutor::dumpCacheStats, "qpo7-cache-stats-dump"));
    }

    private static final class CacheStats {
        final AtomicLong missCount       = new AtomicLong(); // first-call (lambda ran)
        final AtomicLong missTotalNs     = new AtomicLong(); // cumulative first-call time
        final AtomicLong hitCount        = new AtomicLong(); // subsequent cached calls
        final AtomicLong hitTotalNs      = new AtomicLong(); // cumulative cached-call time
    }

    private static CacheStats statsFor(String key) {
        return CACHE_STATS.computeIfAbsent(key, k -> new CacheStats());
    }

    /**
     * Wraps ConcurrentHashMap.computeIfAbsent to separately time the first
     * (cache miss) call from subsequent (cache hit) calls. The miss path runs
     * the supplier and records supplier latency; the hit path records only
     * the map-lookup latency. The difference is the per-query benefit.
     *
     * Instrumentation overhead: two System.nanoTime() calls per invocation
     * plus one AtomicLong.addAndGet — ~50ns total. Immaterial vs the work
     * being measured.
     */
    private static <V> V timedCacheLookup(String key,
                                          ConcurrentHashMap<String, V> cache,
                                          java.util.function.Function<String, V> supplier) {
        CacheStats stats = statsFor(key);
        long t0 = System.nanoTime();

        // Fast-path: present → hit. Race-free sampling: if another thread is
        // mid-miss, we'll block inside computeIfAbsent below and be counted
        // as a hit (we didn't run the lambda). Good enough for measurement.
        V existing = cache.get(key);
        if (existing != null) {
            stats.hitCount.incrementAndGet();
            stats.hitTotalNs.addAndGet(System.nanoTime() - t0);
            return existing;
        }

        // Slow-path: compute (or wait for another thread to compute).
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
            // Another thread filled the entry while we raced — count as hit.
            stats.hitCount.incrementAndGet();
            stats.hitTotalNs.addAndGet(elapsed);
        }
        return value;
    }

    private static void dumpCacheStats() {
        if (CACHE_STATS.isEmpty()) return;
        System.err.println();
        System.err.println("========================================================");
        System.err.println("  QPO-7 Cache Stats (per-key)");
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
            // Savings = hits * (first_call_cost - cached_call_cost)
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

            // ── QPO-3: Projection pushdown ────────────────────────────────────
            int[] projectedColIndices = scanDef.projectedIndices(); // may be null

            List<CatalogColumn> projectedCols;
            byte[]              projectionData;

            if (projectedColIndices != null && projectedColIndices.length > 0) {
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
                projectedCols  = allCols;
                projectionData = null;
            }

            // ── QPO-7: use cached descriptors for the projected column set ────
            // For the scan path, descriptors depend on the projected subset so
            // we cache by "schema.table[col0,col1,...]" to handle both full and
            // projected scans correctly.
            final List<CatalogColumn> projectedColsFinal = projectedCols;
            String descKey = "scan:" + schemaName + "." + tableName
                    + (projectedColIndices != null ? Arrays.toString(projectedColIndices) : "[]");

            List<ColumnDescriptor> descriptors = timedCacheLookup(descKey, descriptorCache, k -> {
                LOGGER.log(INFO, "QPO-7: Building descriptors for " + k + " (first call)");
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

                // ── QPO-7: cache combined join descriptors ────────────────────
                // Both sides are fixed schema — built once per unique join pair.
                final List<CatalogColumn> leftColsFinal  = leftCols;
                final List<CatalogColumn> rightColsFinal = rightCols;
                final int[] allLeftOffsetsFinal  = allLeftOffsets;
                final int[] allRightOffsetsFinal = allRightOffsets;
                final int   remoteRecordSizeFinal = remoteRecordSize;

                String joinDescKey = "join:" + leftSchema + "." + leftTable
                        + "+" + rightSchema + "." + rightTable;

                List<ColumnDescriptor> combinedDescriptors =
                        timedCacheLookup(joinDescKey, descriptorCache, k -> {
                            LOGGER.log(INFO, "QPO-7: Building join descriptors for " + k + " (first call)");
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