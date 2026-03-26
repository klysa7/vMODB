package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick throughput frontier experiment runner.
 *
 * T-clients: HATtrickTClientWorker (new_order + payment, by_name=false).
 *   No pre-generated workload files needed — transactions are generated
 *   on the fly. This avoids the WorkloadUtils NPE that occurred when
 *   txRatioMap was empty, and matches the Phase 1 path exactly.
 *
 * A-clients: HATtrickAClientWorker (HTTP GET to gateway).
 *
 * T-tps: measured via registerBatchCommitConsumer (real committed TIDs).
 * A-qps: measured by sampling HATtrickAClientWorker.getCompletedCount().
 */
public final class HATtrickRunner {

    private static final System.Logger LOG =
            System.getLogger(HATtrickRunner.class.getName());

    private final Coordinator              coordinator;
    private final String                   gatewayBaseUrl;
    private final int[]                    tauValues;
    private final int[]                    alphaValues;
    private final int                      warmupSecs;
    private final int                      measurementSecs;
    private final int                      numWare;
    private final String                   queryPath;

    public record GridPoint(int tau, int alpha, double tTps, double aQps) {
        @Override public String toString() {
            return String.format("τ=%d α=%d  T-tps=%.2f  A-qps=%.4f", tau, alpha, tTps, aQps);
        }
    }

    // Full constructor — txRatio and numTxInputPerType no longer used for T-clients
    // but kept for API compatibility with HATtrickMain
    public HATtrickRunner(Coordinator coordinator,
                          String gatewayBaseUrl,
                          int[] tauValues,
                          int[] alphaValues,
                          int warmupSecs,
                          int measurementSecs,
                          int numWare,
                          Tuple<Integer, String>[] txRatio,
                          Map<String, Integer> numTxInputPerType,
                          String queryPath) {
        this.coordinator     = coordinator;
        this.gatewayBaseUrl  = gatewayBaseUrl;
        this.tauValues       = tauValues;
        this.alphaValues     = alphaValues;
        this.warmupSecs      = warmupSecs;
        this.measurementSecs = measurementSecs;
        this.numWare         = numWare;
        this.queryPath       = queryPath;
    }

    public List<GridPoint> run() throws InterruptedException, IOException {
        List<GridPoint> results = new ArrayList<>();

        System.out.println("\n========================================================");
        System.out.println("  HATtrick Throughput Frontier Experiment");
        System.out.printf ("  Query:  %s%n", queryPath);
        System.out.printf ("  Grid: τ∈%s  α∈%s%n",
                Arrays.toString(tauValues), Arrays.toString(alphaValues));
        System.out.printf ("  Warmup=%ds  Measurement=%ds%n", warmupSecs, measurementSecs);
        System.out.println("========================================================\n");

        // Register batch commit listener — fires on every real commit
        AtomicLong lastCommittedTid = new AtomicLong(0L);
        coordinator.registerBatchCommitConsumer(
                (batchId, lastTid) -> lastCommittedTid.set(lastTid));

        // Pipeline warmup — send a few transactions so the coordinator
        // pipeline is active before the first grid point
        System.out.print("  Warming up pipeline");
        warmupPipeline(lastCommittedTid);
        System.out.println(" ready.\n");

        for (int tau : tauValues) {
            for (int alpha : alphaValues) {
                if (tau == 0 && alpha == 0) {
                    results.add(new GridPoint(0, 0, 0.0, 0.0));
                    continue;
                }
                GridPoint point = runSinglePoint(tau, alpha, lastCommittedTid);
                results.add(point);
                System.out.println("  Result: " + point);
                Thread.sleep(3_000); // drain between grid points
            }
        }

        writeCsv(results);
        printSummary(results);
        return results;
    }

    private GridPoint runSinglePoint(int tau, int alpha,
                                     AtomicLong lastCommittedTid)
            throws InterruptedException {

        System.out.printf("%n--- Running grid point τ=%d α=%d ---%n", tau, alpha);

        // ── Start T-clients (HATtrickTClientWorker) ───────────────────────
        AtomicBoolean tRunning = new AtomicBoolean(true);
        ExecutorService tPool = null;
        List<HATtrickTClientWorker> tWorkers = new ArrayList<>();

        if (tau > 0) {
            tPool = Executors.newFixedThreadPool(tau);
            for (int t = 0; t < tau; t++) {
                HATtrickTClientWorker w = new HATtrickTClientWorker(
                        t, coordinator, tRunning, numWare);
                tWorkers.add(w);
                tPool.submit(w);
            }
        }

        // ── Warmup ────────────────────────────────────────────────────────
        System.out.printf("  Warmup %ds...%n", warmupSecs);
        Thread.sleep(warmupSecs * 1_000L);

        // ── Snapshot at start of measurement window ───────────────────────
        long tStart      = lastCommittedTid.get();
        long windowStart = System.currentTimeMillis();

        // ── Start A-clients ───────────────────────────────────────────────
        AtomicBoolean aRunning = new AtomicBoolean(true);
        ExecutorService aPool = alpha > 0 ? Executors.newFixedThreadPool(alpha) : null;
        List<HATtrickAClientWorker> aWorkers = new ArrayList<>();
        for (int i = 0; i < alpha; i++) {
            HATtrickAClientWorker w = new HATtrickAClientWorker(
                    i, gatewayBaseUrl, queryPath, aRunning);
            aWorkers.add(w);
            aPool.submit(w);
        }

        long aStart = aWorkers.stream()
                .mapToLong(HATtrickAClientWorker::getCompletedCount).sum();

        // ── Measurement window ────────────────────────────────────────────
        System.out.printf("  Measuring %ds...%n", measurementSecs);
        Thread.sleep(measurementSecs * 1_000L);

        // ── Snapshots at end ──────────────────────────────────────────────
        long tEnd      = lastCommittedTid.get();
        long aEnd      = aWorkers.stream()
                .mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowEnd = System.currentTimeMillis();

        // ── Stop A-clients ─────────────────────────────────────────────────
        aRunning.set(false);
        if (aPool != null) {
            aPool.shutdownNow();
            aPool.awaitTermination(3, TimeUnit.SECONDS);
        }

        // ── Stop T-clients ─────────────────────────────────────────────────
        tRunning.set(false);
        if (tPool != null) {
            tPool.shutdownNow();
            tPool.awaitTermination(3, TimeUnit.SECONDS);
        }

        // ── Compute throughputs ───────────────────────────────────────────
        double elapsedSec        = (windowEnd - windowStart) / 1000.0;
        long   committedInWindow = tEnd - tStart;
        double tTps              = committedInWindow / elapsedSec;
        double aQps              = (aEnd - aStart) / elapsedSec;

        System.out.printf("  τ=%d α=%d  T-tps=%.2f  A-qps=%.4f  (window=%.1fs, committed=%d)%n",
                tau, alpha, tTps, aQps, elapsedSec, committedInWindow);

        return new GridPoint(tau, alpha, tTps, aQps);
    }

    // ── Pipeline warmup ───────────────────────────────────────────────────────
    // Sends 100 transactions via T-client worker to prime the coordinator
    // pipeline before the first grid point measurement.

    private void warmupPipeline(AtomicLong lastCommittedTid) throws InterruptedException {
        AtomicBoolean warmupRunning = new AtomicBoolean(true);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        HATtrickTClientWorker warmupWorker = new HATtrickTClientWorker(
                0, coordinator, warmupRunning, numWare);
        pool.submit(warmupWorker);

        // Wait up to 30s for the first real commit
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(1_000);
            System.out.print(".");
            if (lastCommittedTid.get() > 0) break;
        }

        // Let it run 2 more seconds to stabilize
        Thread.sleep(2_000);

        warmupRunning.set(false);
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        // Drain: wait until pipeline quiets
        long prev = lastCommittedTid.get();
        int stableCount = 0;
        while (stableCount < 2) {
            Thread.sleep(1_000);
            long now = lastCommittedTid.get();
            if (now == prev) stableCount++;
            else { stableCount = 0; prev = now; }
        }
    }

    // ── CSV + summary ─────────────────────────────────────────────────────────

    private void writeCsv(List<GridPoint> results) throws IOException {
        String ts = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String queryLabel = queryPath.replace("/olap/", "").replace(".", "_").replace("/", "_");
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
                .max(Comparator.comparingDouble(GridPoint::tTps))
                .ifPresent(p -> System.out.printf(
                        "%nPure OLTP baseline (α=0): X^T = %.2f tps (τ=%d)%n",
                        p.tTps(), p.tau()));
        results.stream()
                .filter(p -> p.tau() == 0 && p.alpha() > 0)
                .max(Comparator.comparingDouble(GridPoint::aQps))
                .ifPresent(p -> System.out.printf(
                        "Pure OLAP baseline (τ=0): X^A = %.4f qps (α=%d)%n",
                        p.aQps(), p.alpha()));
    }
}