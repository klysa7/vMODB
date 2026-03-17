package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HATtrick throughput frontier experiment runner.
 *
 * Implements the saturation method from Section 3.3 of the HATtrick paper:
 *
 *   1. Find τ_max (pure OLTP baseline):
 *      run with α=0, τ ∈ {1,2,4,8,...} until TPS plateaus.
 *
 *   2. Find α_max (pure OLAP baseline):
 *      run with τ=0, α ∈ {1,2,4,...} until QPS plateaus.
 *
 *   3. Build fixed-T lines: for each τ in tauValues, vary α ∈ alphaValues.
 *      Each (τ,α) point is one data point on the grid.
 *
 *   4. Build fixed-A lines: for each α in alphaValues, vary τ ∈ tauValues.
 *
 *   The full grid is written to a CSV file. The throughput frontier is the
 *   upper-right envelope of all grid points — computed offline from the CSV
 *   (e.g. in Python/matplotlib).
 *
 * Freshness is NOT measured. The professor confirmed vMODB always achieves
 * freshness=0 by design (reads live MVCC data), so there is nothing to measure.
 *
 * Usage:
 *   int[] tauValues   = {0, 1, 2, 4};   // number of T-clients
 *   int[] alphaValues = {0, 1, 2, 4};   // number of A-clients
 *   HATtrickRunner runner = new HATtrickRunner(coordinator, "http://localhost:8095",
 *       tauValues, alphaValues, warmupSecs=5, measurementSecs=30, numWarehouses=1);
 *   runner.run();
 */
public final class HATtrickRunner {

    private static final System.Logger LOG =
            System.getLogger(HATtrickRunner.class.getName());

    private final Coordinator coordinator;
    private final String      gatewayBaseUrl;
    private final int[]       tauValues;        // T-client counts to test
    private final int[]       alphaValues;      // A-client counts to test
    private final int         warmupSecs;
    private final int         measurementSecs;
    private final int         numWarehouses;

    // ── Result record ────────────────────────────────────────────────────────

    public record GridPoint(int tau, int alpha, double tTps, double aQps) {
        @Override
        public String toString() {
            return String.format("τ=%d α=%d  T-tps=%.2f  A-qps=%.4f", tau, alpha, tTps, aQps);
        }
    }

    public HATtrickRunner(Coordinator coordinator,
                          String gatewayBaseUrl,
                          int[] tauValues,
                          int[] alphaValues,
                          int warmupSecs,
                          int measurementSecs,
                          int numWarehouses) {
        this.coordinator     = coordinator;
        this.gatewayBaseUrl  = gatewayBaseUrl;
        this.tauValues       = tauValues;
        this.alphaValues     = alphaValues;
        this.warmupSecs      = warmupSecs;
        this.measurementSecs = measurementSecs;
        this.numWarehouses   = numWarehouses;
    }

    // ── Main entry ────────────────────────────────────────────────────────────

    public List<GridPoint> run() throws InterruptedException, IOException {
        List<GridPoint> results = new ArrayList<>();

        System.out.println("\n========================================================");
        System.out.println("  HATtrick Throughput Frontier Experiment");
        System.out.printf ("  Grid: τ∈%s  α∈%s%n",
                java.util.Arrays.toString(tauValues),
                java.util.Arrays.toString(alphaValues));
        System.out.printf ("  Warmup=%ds  Measurement=%ds%n", warmupSecs, measurementSecs);
        System.out.println("========================================================\n");

        // ── Pipeline warmup ───────────────────────────────────────────────
        // A fresh coordinator must complete at least one full transaction
        // round-trip before getNumTIDsCommitted() advances past 0.
        // Submit transactions and wait until the first batch actually commits.
        System.out.print("  Waiting for batch pipeline to sync");
        warmupPipeline();
        System.out.println(" ready.\n");

        for (int tau : tauValues) {
            for (int alpha : alphaValues) {
                // skip (0,0) — nothing runs, trivially (0,0)
                if (tau == 0 && alpha == 0) {
                    results.add(new GridPoint(0, 0, 0.0, 0.0));
                    continue;
                }
                GridPoint point = runSinglePoint(tau, alpha);
                results.add(point);
                System.out.println("  Result: " + point);
                // brief pause between grid points so the system drains
                Thread.sleep(2_000);
            }
        }

        writeCsv(results);
        printSummary(results);
        return results;
    }

    // ── Single (τ, α) measurement ─────────────────────────────────────────────

    private GridPoint runSinglePoint(int tau, int alpha) throws InterruptedException {
        System.out.printf("%n--- Running grid point τ=%d α=%d ---%n", tau, alpha);

        AtomicBoolean running = new AtomicBoolean(true);
        ExecutorService pool = Executors.newFixedThreadPool(tau + alpha + 1);

        // ── Start T-clients ────────────────────────────────────────────────
        List<HATtrickTClientWorker> tWorkers = new ArrayList<>(tau);
        for (int i = 0; i < tau; i++) {
            HATtrickTClientWorker w = new HATtrickTClientWorker(
                    i, coordinator, running, numWarehouses);
            tWorkers.add(w);
            pool.submit(w);
        }

        // ── Start A-clients ────────────────────────────────────────────────
        List<HATtrickAClientWorker> aWorkers = new ArrayList<>(alpha);
        for (int i = 0; i < alpha; i++) {
            HATtrickAClientWorker w = new HATtrickAClientWorker(
                    i, gatewayBaseUrl, running);
            aWorkers.add(w);
            pool.submit(w);
        }

        // ── Warmup ────────────────────────────────────────────────────────
        System.out.printf("  Warmup %ds...%n", warmupSecs);
        Thread.sleep(warmupSecs * 1_000L);

        // ── Snapshot at start of measurement window ───────────────────────
        // coordinator.getNumTIDsCommitted() returns the lastTid of the last
        // fully committed batch — same value ExperimentUtils uses internally.
        // Sampling before and after the window gives us exactly the committed
        // transactions during the window without any callback wiring.
        long tCommittedStart = coordinator.getNumTIDsCommitted();
        long aStart          = aWorkers.stream().mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowStartMs   = System.currentTimeMillis();

        System.out.printf("  Measuring %ds...%n", measurementSecs);
        Thread.sleep(measurementSecs * 1_000L);

        // ── Snapshot at end of measurement window ─────────────────────────
        long tCommittedEnd = coordinator.getNumTIDsCommitted();
        long aEnd          = aWorkers.stream().mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowEndMs   = System.currentTimeMillis();

        // ── Stop all workers ──────────────────────────────────────────────
        running.set(false);
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // ── Compute throughputs ───────────────────────────────────────────
        double elapsedSec      = (windowEndMs - windowStartMs) / 1000.0;
        long   committedInWindow = tCommittedEnd - tCommittedStart;
        double tTps            = committedInWindow / elapsedSec;
        double aQps            = (aEnd - aStart) / elapsedSec;

        System.out.printf("  τ=%d α=%d  T-tps=%.2f  A-qps=%.4f  (window=%.1fs, committed=%d)%n",
                tau, alpha, tTps, aQps, elapsedSec, committedInWindow);

        return new GridPoint(tau, alpha, tTps, aQps);
    }

    // ── Pipeline warmup ───────────────────────────────────────────────────────

    /**
     * Submits transactions continuously until at least one batch commits,
     * confirming the full round-trip (coordinator → VMSes → coordinator) works.
     * Times out after 60s with a warning rather than blocking forever.
     */
    private void warmupPipeline() throws InterruptedException {
        AtomicBoolean warmupRunning = new AtomicBoolean(true);
        ExecutorService warmupPool = Executors.newSingleThreadExecutor();
        HATtrickTClientWorker warmupWorker = new HATtrickTClientWorker(
                0, coordinator, warmupRunning, numWarehouses);
        warmupPool.submit(warmupWorker);

        long deadline = System.currentTimeMillis() + 60_000;
        long initialCommitted = coordinator.getNumTIDsCommitted();
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(1_000);
            System.out.print(".");
            long now = coordinator.getNumTIDsCommitted();
            if (now > initialCommitted) {
                // At least one batch committed — pipeline is warm.
                // Let it run 2 more seconds so the next batch is also ready.
                Thread.sleep(2_000);
                break;
            }
        }
        warmupRunning.set(false);
        warmupPool.shutdownNow();
        warmupPool.awaitTermination(3, TimeUnit.SECONDS);
    }

    // ── CSV output ────────────────────────────────────────────────────────────

    private void writeCsv(List<GridPoint> results) throws IOException {
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String filename = "hattrick_frontier_" + ts + ".csv";

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

        // Print the pure baselines clearly
        results.stream()
                .filter(p -> p.alpha() == 0 && p.tau() > 0)
                .max((a, b) -> Double.compare(a.tTps(), b.tTps()))
                .ifPresent(p -> System.out.printf("%nPure OLTP baseline (α=0): X^T = %.2f tps (τ=%d)%n",
                        p.tTps(), p.tau()));
        results.stream()
                .filter(p -> p.tau() == 0 && p.alpha() > 0)
                .max((a, b) -> Double.compare(a.aQps(), b.aQps()))
                .ifPresent(p -> System.out.printf("Pure OLAP baseline (τ=0): X^A = %.4f qps (α=%d)%n",
                        p.aQps(), p.alpha()));
    }
}