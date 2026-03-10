package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HATtrick throughput frontier runner — saturation method (Section 3.3).
 *
 * FIX FOR PROBLEM 5 (shutdown):
 *   Uses pool.shutdownNow() instead of pool.shutdown(). This sends
 *   Thread.interrupt() to all worker threads, causing TClientWorker and
 *   AClientWorker to exit their loops immediately rather than waiting for
 *   the next running.get() check. awaitTermination reduced to 5s.
 *
 * FIX FOR PROBLEM 1 (commit time):
 *   Calls coordinator.setFreshnessTracker(tracker) before starting workers
 *   so VmsTransactionCoordinator routes batch-commit callbacks to the
 *   correct per-run FreshnessTracker instance.
 *
 * FIX FOR PROBLEM 4 (order_line overflow):
 *   Set max_records.order_line=10000000 in order VMS application.properties.
 *   This class has no code change needed for that fix.
 */
public final class HATtrickRunner {

    private static final System.Logger LOG = System.getLogger(HATtrickRunner.class.getName());

    private final int    measurementWindowSec;
    private final int    warmupSec;
    private final int    tauMax;
    private final int    alphaMax;
    private final int    numWarehouses;
    private final String gatewayUrl;
    private final String olapOrderUrl;
    private final String outputCsvPath;
    private final TransactionCoordinator coordinator;

    public HATtrickRunner(TransactionCoordinator coordinator,
                          String gatewayUrl,
                          String olapOrderUrl,
                          int tauMax,
                          int alphaMax,
                          int numWarehouses,
                          int measurementWindowSec,
                          int warmupSec,
                          String outputCsvPath) {
        this.coordinator          = coordinator;
        this.gatewayUrl           = gatewayUrl;
        this.olapOrderUrl         = olapOrderUrl;
        this.tauMax               = tauMax;
        this.alphaMax             = alphaMax;
        this.numWarehouses        = numWarehouses;
        this.measurementWindowSec = measurementWindowSec;
        this.warmupSec            = warmupSec;
        this.outputCsvPath        = outputCsvPath;
    }

    public List<ExperimentPoint> runFullGrid() throws Exception {
        List<ExperimentPoint> results = new ArrayList<>();
        int steps = 6;

        // Fixed-T lines
        for (int ti = 1; ti <= steps; ti++) {
            int tau = Math.max(1, (tauMax * ti) / steps);
            for (int ai = 0; ai <= steps; ai++) {
                int alpha = (alphaMax * ai) / steps;
                if (tau == 0 && alpha == 0) continue;
                results.add(runSingle(tau, alpha));
            }
        }
        // Fixed-A lines
        for (int ai = 1; ai <= steps; ai++) {
            int alpha = Math.max(1, (alphaMax * ai) / steps);
            for (int ti = 0; ti <= steps; ti++) {
                int tau = (tauMax * ti) / steps;
                if (tau == 0 && alpha == 0) continue;
                results.add(runSingle(tau, alpha));
            }
        }

        writeCsv(results);
        return results;
    }

    public List<ExperimentPoint> runQuickGrid() throws Exception {
        List<ExperimentPoint> results = new ArrayList<>();
        int[] taus   = {0, Math.max(1, tauMax / 2), tauMax};
        int[] alphas = {0, Math.max(1, alphaMax / 2), alphaMax};

        for (int tau : taus) {
            for (int alpha : alphas) {
                if (tau == 0 && alpha == 0) continue;
                results.add(runSingle(tau, alpha));
            }
        }
        writeCsv(results);
        return results;
    }

    public ExperimentPoint runSingle(int tau, int alpha) throws Exception {
        LOG.log(System.Logger.Level.INFO,
                "Running τ={0} α={1} (warmup={2}s measure={3}s)",
                tau, alpha, warmupSec, measurementWindowSec);

        AtomicBoolean running = new AtomicBoolean(true);
        FreshnessTracker tracker = new FreshnessTracker(tau);

        // ── FIX: wire this run's tracker into the coordinator BEFORE starting workers
        coordinator.setFreshnessTracker(tracker);

        // Build workers — TClientWorker no longer takes freshnessTracker directly
        List<TClientWorker> tWorkers = new ArrayList<>();
        for (int j = 1; j <= tau; j++) {
            tWorkers.add(new TClientWorker(j, coordinator, running, numWarehouses));
        }

        List<AClientWorker> aWorkers = new ArrayList<>();
        for (int j = 1; j <= alpha; j++) {
            aWorkers.add(new AClientWorker(j, gatewayUrl, olapOrderUrl, tau, tracker, running));
        }

        ExecutorService pool = Executors.newFixedThreadPool(tau + alpha);
        tWorkers.forEach(pool::submit);
        aWorkers.forEach(pool::submit);

        // Warm-up
        if (warmupSec > 0) {
            Thread.sleep(warmupSec * 1000L);
        }

        long measureStart = System.nanoTime();

        // Measurement window
        Thread.sleep(measurementWindowSec * 1000L);

        // ── FIX: shutdownNow() sends interrupt to all threads → fast stop
        running.set(false);
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);

        // Detach tracker so stray batch-commit callbacks don't pollute the next run
        coordinator.setFreshnessTracker(null);

        double durationSec = (System.nanoTime() - measureStart) / 1e9;

        double totalTps = tWorkers.stream().mapToDouble(TClientWorker::throughputTps).sum();
        double totalQps = aWorkers.stream().mapToDouble(AClientWorker::throughputQps).sum();

        tracker.printSummary(String.format("τ=%d α=%d", tau, alpha));

        return new ExperimentPoint(
                tau, alpha,
                totalTps, totalQps,
                tracker.averageFreshnessSeconds(),
                tracker.p99FreshnessSeconds(),
                durationSec
        );
    }

    private void writeCsv(List<ExperimentPoint> results) throws IOException {
        try (PrintWriter pw = new PrintWriter(new FileWriter(outputCsvPath))) {
            pw.println("# vMODB HATtrick Results — " + Instant.now());
            pw.println(ExperimentPoint.csvHeader());
            for (ExperimentPoint pt : results) pw.println(pt.toCsv());
        }
        System.out.println("[HATtrick] Results written to " + outputCsvPath);
    }
}