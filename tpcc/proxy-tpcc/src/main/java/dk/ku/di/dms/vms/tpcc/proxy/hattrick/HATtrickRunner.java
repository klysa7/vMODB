package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick throughput frontier experiment runner.
 *
 * Uses the PROVEN TPC-C path (WorkloadUtils.submitWorkload) for T-clients
 * — the same path as option 4. This guarantees real committed TPS.
 *
 * For each (τ, α) grid point:
 *   - τ T-workers submit new_order transactions via WorkloadUtils.submitWorkload
 *   - α A-workers fire GET /olap/q1 via HATtrickAClientWorker
 *   - T-tps measured via registerBatchCommitConsumer (fires on real commits)
 *   - A-qps measured by sampling HATtrickAClientWorker.getCompletedCount()
 */
public final class HATtrickRunner {

    private static final System.Logger LOG =
            System.getLogger(HATtrickRunner.class.getName());

    private final Coordinator               coordinator;
    private final String                    gatewayBaseUrl;
    private final int[]                     tauValues;
    private final int[]                     alphaValues;
    private final int                       warmupSecs;
    private final int                       measurementSecs;
    private final int                       numWare;
    private final Tuple<Integer, String>[]  txRatio;
    private final Map<String, Integer>      txRatioMap;
    private final String                    queryPath;  // e.g. "/olap/q1.1"

    // ── Result record ─────────────────────────────────────────────────────────

    public record GridPoint(int tau, int alpha, double tTps, double aQps) {
        @Override public String toString() {
            return String.format("τ=%d α=%d  T-tps=%.2f  A-qps=%.4f", tau, alpha, tTps, aQps);
        }
    }

    public HATtrickRunner(Coordinator coordinator,
                          String gatewayBaseUrl,
                          int[] tauValues,
                          int[] alphaValues,
                          int warmupSecs,
                          int measurementSecs,
                          int numWare,
                          Tuple<Integer, String>[] txRatio,
                          Map<String, Integer> txRatioMap,
                          String queryPath) {
        this.coordinator    = coordinator;
        this.gatewayBaseUrl = gatewayBaseUrl;
        this.tauValues      = tauValues;
        this.alphaValues    = alphaValues;
        this.warmupSecs     = warmupSecs;
        this.measurementSecs = measurementSecs;
        this.numWare        = numWare;
        this.txRatio        = txRatio;
        this.txRatioMap     = txRatioMap;
        this.queryPath      = queryPath;
    }

    /** Backwards-compatible constructor — defaults to Q1 */
    public HATtrickRunner(Coordinator coordinator,
                          String gatewayBaseUrl,
                          int[] tauValues,
                          int[] alphaValues,
                          int warmupSecs,
                          int measurementSecs,
                          int numWare,
                          Tuple<Integer, String>[] txRatio,
                          Map<String, Integer> txRatioMap) {
        this(coordinator, gatewayBaseUrl, tauValues, alphaValues,
                warmupSecs, measurementSecs, numWare, txRatio, txRatioMap, "/olap/q1");
    }

    // ── Main entry ────────────────────────────────────────────────────────────

    public List<GridPoint> run() throws InterruptedException, IOException {
        List<GridPoint> results = new ArrayList<>();

        System.out.println("\n========================================================");
        System.out.println("  HATtrick Throughput Frontier Experiment");
        System.out.printf ("  Query:  %s%n", queryPath);
        System.out.printf ("  Grid: τ∈%s  α∈%s%n",
                Arrays.toString(tauValues), Arrays.toString(alphaValues));
        System.out.printf ("  Warmup=%ds  Measurement=%ds%n", warmupSecs, measurementSecs);
        System.out.println("  Note: longer measurement windows reduce batch boundary noise.");
        System.out.println("========================================================\n");

        // ── Generate workload input files ─────────────────────────────────
        // Create input files for numWare warehouses. τ workers share these
        // files — each worker reads sequentially, so τ=2 means 2 workers
        // each reading from their own per-warehouse file set.
        // We use numWare=1 from properties, and for τ>1 workers all read
        // from the same file by loading fresh iterators per worker.
        // To support τ=2, we generate files for max(tauValues) warehouses.
        int maxTau = Arrays.stream(tauValues).max().orElse(1);
        int wares  = Math.max(maxTau, numWare);

        System.out.printf("  Generating workload input files for %d warehouse(s)...%n", wares);
        try {
            WorkloadUtils.createWorkload(wares, false, txRatioMap);
            System.out.println("  Workload files generated.\n");
        } catch (IOException e) {
            System.out.println("ERROR generating workload: " + e.getMessage());
            return results;
        }

        // ── Register ONE batch commit listener for the entire experiment ──
        // Fires ONLY after all terminal VMSes respond with BatchComplete.
        AtomicLong lastCommittedTid = new AtomicLong(0L);
        coordinator.registerBatchCommitConsumer(
                (batchId, lastTid) -> lastCommittedTid.set(lastTid));

        // ── Pipeline warmup ───────────────────────────────────────────────
        System.out.print("  Waiting for batch pipeline to sync");
        warmupPipeline(wares, lastCommittedTid);
        System.out.println(" ready.\n");

        for (int tau : tauValues) {
            for (int alpha : alphaValues) {
                if (tau == 0 && alpha == 0) {
                    results.add(new GridPoint(0, 0, 0.0, 0.0));
                    continue;
                }
                GridPoint point = runSinglePoint(tau, alpha, wares, lastCommittedTid);
                results.add(point);
                System.out.println("  Result: " + point);
                // Drain 3s between grid points so in-flight transactions from
                // the previous point clear the VMS channels before the next starts.
                Thread.sleep(3_000);
            }
        }

        writeCsv(results);
        printSummary(results);
        return results;
    }

    // ── Single (τ, α) measurement ─────────────────────────────────────────────

    private GridPoint runSinglePoint(int tau, int alpha, int wares,
                                     AtomicLong lastCommittedTid) throws InterruptedException {
        System.out.printf("%n--- Running grid point τ=%d α=%d ---%n", tau, alpha);

        int totalDurationMs = (warmupSecs + measurementSecs) * 1_000;

        Future<?> tFuture = null;
        ExecutorService tPool = null;

        if (tau > 0) {
            // Always load numWare=1 file set and duplicate for each τ worker.
            // Loading tau=2 file sets would include warehouse_2 transactions
            // (w_id=2) which abort because only warehouse 1 has data.
            // Each worker needs its own independent iterator, so we regenerate
            // the warehouse_1 file tau times and collect all iterator sets.
            var input = new ArrayList<Map<String, Iterator<Object>>>();
            for (int w = 0; w < tau; w++) {
                try { WorkloadUtils.createWorkload(numWare, false, txRatioMap); }
                catch (IOException ignored) {}
                input.addAll(WorkloadUtils.mapWorkloadInputFiles(numWare, txRatioMap));
            }

            final List<Map<String, Iterator<Object>>> finalInput = input;
            final Tuple<Integer, String>[] finalTxRatio = txRatio;
            final int finalDuration = totalDurationMs;

            tPool = Executors.newSingleThreadExecutor();
            tFuture = tPool.submit(() ->
                    WorkloadUtils.submitWorkload(finalTxRatio, finalInput,
                            txInput -> {
                                dk.ku.di.dms.vms.coordinator.transaction.TransactionInput ti =
                                        buildTransactionInput(txInput);
                                coordinator.queueTransactionInput(ti);
                                // Throttle to ~1000 submissions/sec per worker.
                                // Without this, WorkloadUtils submits 200,000 transactions
                                // in < 1 second, filling 10,000-transaction batches that
                                // cause the order VMS to insert ~100,000 order_line rows
                                // at once and crash with Broken pipe.
                                // 1ms sleep → max 1000 tx/sec → max ~500 tx per batch
                                // window (batch_window_ms=500) → order VMS stays healthy.
                                try { Thread.sleep(1); } catch (InterruptedException ignored) {}
                                return (long) 0;
                            },
                            finalDuration)
            );
        }

        // ── Warmup ────────────────────────────────────────────────────────
        // T-workers run during warmup (already started above).
        // A-clients are NOT started yet — they should only run during
        // the measurement window, not waste queries on warmup time.
        System.out.printf("  Warmup %ds...%n", warmupSecs);
        Thread.sleep(warmupSecs * 1_000L);

        // ── Start A-clients at the beginning of measurement window ────────
        AtomicBoolean aRunning = new AtomicBoolean(true);
        ExecutorService aPool = alpha > 0
                ? Executors.newFixedThreadPool(alpha)
                : null;
        List<HATtrickAClientWorker> aWorkers = new ArrayList<>(alpha);
        for (int i = 0; i < alpha; i++) {
            HATtrickAClientWorker w = new HATtrickAClientWorker(
                    i, gatewayBaseUrl, queryPath, aRunning);
            aWorkers.add(w);
            aPool.submit(w);
        }

        // ── Snapshot at start of measurement window ───────────────────────
        long tStart      = lastCommittedTid.get();
        long aStart      = aWorkers.stream().mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowStart = System.currentTimeMillis();

        System.out.printf("  Measuring %ds...%n", measurementSecs);
        Thread.sleep(measurementSecs * 1_000L);

        // ── Snapshot at end of measurement window ─────────────────────────
        long tEnd      = lastCommittedTid.get();
        long aEnd      = aWorkers.stream().mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowEnd = System.currentTimeMillis();

        // ── Stop A-clients ─────────────────────────────────────────────────
        aRunning.set(false);
        if (aPool != null) {
            aPool.shutdownNow();
            aPool.awaitTermination(3, TimeUnit.SECONDS);
        }

        // ── Wait for T-workers to finish ───────────────────────────────────
        if (tFuture != null) {
            try { tFuture.get(10, TimeUnit.SECONDS); } catch (Exception ignored) {}
            tPool.shutdownNow();
            tPool.awaitTermination(3, TimeUnit.SECONDS);
        }

        // ── Compute throughputs ───────────────────────────────────────────
        double elapsedSec       = (windowEnd - windowStart) / 1000.0;
        long   committedInWindow = tEnd - tStart;
        double tTps             = committedInWindow / elapsedSec;
        double aQps             = (aEnd - aStart) / elapsedSec;

        System.out.printf("  τ=%d α=%d  T-tps=%.2f  A-qps=%.4f  (window=%.1fs, committed=%d)%n",
                tau, alpha, tTps, aQps, elapsedSec, committedInWindow);

        return new GridPoint(tau, alpha, tTps, aQps);
    }

    // ── Transaction input builder ─────────────────────────────────────────────

    private dk.ku.di.dms.vms.coordinator.transaction.TransactionInput buildTransactionInput(Object txInput) {
        dk.ku.di.dms.vms.coordinator.transaction.TransactionInput.Event eventPayload;
        String txIdentifier;
        if (txInput instanceof dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn newOrderInput) {
            txIdentifier = "new_order";
            eventPayload = new dk.ku.di.dms.vms.coordinator.transaction.TransactionInput.Event(
                    "new-order-ware-in", newOrderInput.toString());
        } else if (txInput instanceof dk.ku.di.dms.vms.tpcc.common.events.PaymentIn paymentInput) {
            txIdentifier = "payment";
            eventPayload = new dk.ku.di.dms.vms.coordinator.transaction.TransactionInput.Event(
                    "payment-in", paymentInput.toString());
        } else {
            txIdentifier = "order_status";
            eventPayload = new dk.ku.di.dms.vms.coordinator.transaction.TransactionInput.Event(
                    "order-status-in", txInput.toString());
        }
        return new dk.ku.di.dms.vms.coordinator.transaction.TransactionInput(txIdentifier, eventPayload);
    }

    // ── Pipeline warmup ───────────────────────────────────────────────────────

    private void warmupPipeline(int wares, AtomicLong lastCommittedTid)
            throws InterruptedException {
        // Generate a tiny 100-transaction warmup workload — separate from the
        // main experiment files. 100 transactions is enough to confirm one batch
        // commits without flooding the order VMS (which crashes if it receives
        // 200,000 order_line inserts at once with no sleep between submissions).
        Map<String, Iterator<Object>> warmupTxCount = new java.util.TreeMap<>();
        Map<String, Integer> warmupSizeMap = new java.util.TreeMap<>();
        warmupSizeMap.put("new_order", 100);

        try { WorkloadUtils.createWorkload(1, false, warmupSizeMap); }
        catch (IOException e) {
            System.out.println("WARNING: warmup workload generation failed: " + e.getMessage());
            return;
        }

        List<Map<String, Iterator<Object>>> warmupInput =
                WorkloadUtils.mapWorkloadInputFiles(1, warmupSizeMap);

        @SuppressWarnings("unchecked")
        Tuple<Integer, String>[] warmupRatio = new Tuple[]{Tuple.of(100, "new_order")};

        AtomicBoolean warmupStop = new AtomicBoolean(false);
        ExecutorService pool = Executors.newSingleThreadExecutor();

        pool.submit(() ->
                WorkloadUtils.submitWorkload(warmupRatio, warmupInput,
                        txInput -> {
                            if (warmupStop.get()) return 0L;
                            coordinator.queueTransactionInput(buildTransactionInput(txInput));
                            return 0L;
                        },
                        30_000)
        );

        // Wait up to 60s for the first real commit
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(1_000);
            System.out.print(".");
            if (lastCommittedTid.get() > 0) break;
        }

        warmupStop.set(true);
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        // Drain: wait until pipeline quiets (2 stable seconds)
        long prev = lastCommittedTid.get();
        int stableCount = 0;
        while (stableCount < 2) {
            Thread.sleep(1_000);
            long now = lastCommittedTid.get();
            if (now == prev) stableCount++;
            else { stableCount = 0; prev = now; }
        }

        // Regenerate the real experiment workload files (warmup consumed warehouse_1 file)
        try { WorkloadUtils.createWorkload(wares, false, txRatioMap); }
        catch (IOException e) {
            System.out.println("WARNING: could not regenerate workload after warmup: " + e.getMessage());
        }
        System.out.print(" (drained)");
    }

    // ── CSV output ────────────────────────────────────────────────────────────

    private void writeCsv(List<GridPoint> results) throws IOException {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String queryLabel = queryPath.replace("/olap/", "").replace(".", "_");
        String filename = "hattrick_" + queryLabel + "_" + ts + ".csv";
        try (BufferedWriter w = new BufferedWriter(new FileWriter(filename))) {
            w.write("tau,alpha,t_tps,a_qps");
            w.newLine();
            for (GridPoint p : results) {
                w.write(String.format("%d,%d,%.4f,%.4f",
                        p.tau(), p.alpha(), p.tTps(), p.aQps()));
                w.newLine();
            }
        }
        System.out.println("\nResults written to: " + filename);
    }

    private void printSummary(List<GridPoint> results) {
        System.out.println("\n========= THROUGHPUT FRONTIER GRID =========");
        System.out.printf("%-6s %-6s %-12s %-12s%n", "tau", "alpha", "T-tps", "A-qps");
        System.out.println("---------------------------------------------");
        for (GridPoint p : results) {
            System.out.printf("%-6d %-6d %-12.2f %-12.4f%n",
                    p.tau(), p.alpha(), p.tTps(), p.aQps());
        }
        System.out.println("=============================================");
        results.stream()
                .filter(p -> p.alpha() == 0 && p.tau() > 0)
                .max((a, b) -> Double.compare(a.tTps(), b.tTps()))
                .ifPresent(p -> System.out.printf(
                        "%nPure OLTP baseline (α=0): X^T = %.2f tps (τ=%d)%n",
                        p.tTps(), p.tau()));
        results.stream()
                .filter(p -> p.tau() == 0 && p.alpha() > 0)
                .max((a, b) -> Double.compare(a.aQps(), b.aQps()))
                .ifPresent(p -> System.out.printf(
                        "Pure OLAP baseline (τ=0): X^A = %.4f qps (α=%d)%n",
                        p.aQps(), p.alpha()));
    }
}