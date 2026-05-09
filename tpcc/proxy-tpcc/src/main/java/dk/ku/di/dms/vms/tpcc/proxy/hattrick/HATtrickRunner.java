package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick throughput frontier experiment runner.
 *
 * T-clients: HATtrickTClientWorker (new_order + payment, by_name=false).
 * A-clients: HATtrickAClientWorker (HTTP GET to gateway).
 *
 * T-tps: measured via registerBatchCommitConsumer (real committed TIDs).
 * A-qps: measured by sampling HATtrickAClientWorker.getCompletedCount().
 *
 * THROTTLE
 *   Throttling is performed at the coordinator level (TransactionWorker
 *   sleeps after each batch is sealed, controlled by the batch_sleep_ms
 *   property). The runner does not need to do anything throttle-specific.
 *
 * PER-CLIENT INFLIGHT BUDGET
 *   Each T-client is given its own 5000-tx inflight budget. The global cap
 *   passed to every worker in a point is MAX_IN_FLIGHT_PER_CLIENT * tau.
 *
 * DRAIN BEHAVIOR
 *   The drain has three phases (pre-queue plateau, inflight wait,
 *   post-drain settle) that protect against cascade failures across cells.
 */
public final class HATtrickRunner {

    private static final System.Logger LOG =
            System.getLogger(HATtrickRunner.class.getName());

    /** Per-T-client inflight budget. Global backpressure cap = this × tau. */
    private static final int MAX_IN_FLIGHT_PER_CLIENT = 5_000;

    /** Bounded inter-point drain timeout (ms). */
    private static final long MAX_DRAIN_MS = 180_000L;

    /** Stall threshold — how long without progress before giving up. */
    private static final long STALL_THRESHOLD_MS = 45_000L;

    /** Settle period after drain completes. */
    private static final long POST_DRAIN_SETTLE_MS = 15_000L;

    /** Wait this long for coordinator's pre-queue submitted count to plateau. */
    private static final long PRE_QUEUE_PLATEAU_MS = 5_000L;

    private final Coordinator coordinator;
    private final String      gatewayBaseUrl;
    private final int[]       tauValues;
    private final int[]       alphaValues;
    private final int         warmupSecs;
    private final int         measurementSecs;
    private final int         numWare;
    private final String      queryPath;

    public record GridPoint(int tau, int alpha, double tTps, double aQps,
                            boolean saturated) {
        public GridPoint(int tau, int alpha, double tTps, double aQps) {
            this(tau, alpha, tTps, aQps, false);
        }

        @Override public String toString() {
            String base = String.format("τ=%d α=%d  T-tps=%.2f  A-qps=%.4f",
                    tau, alpha, tTps, aQps);
            return saturated ? base + "  [SATURATED]" : base;
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
        System.out.printf ("  Per-T-client inflight budget: %d%n", MAX_IN_FLIGHT_PER_CLIENT);
        System.out.println("  Throttle: applied at coordinator (batch_sleep_ms in app.properties)");
        System.out.printf ("  Drain timeout: %ds  Stall threshold: %ds  Settle: %ds%n",
                MAX_DRAIN_MS / 1000, STALL_THRESHOLD_MS / 1000, POST_DRAIN_SETTLE_MS / 1000);
        System.out.println("========================================================\n");

        AtomicLong lastCommittedTid = new AtomicLong(0L);
        coordinator.registerBatchCommitConsumer(
                (batchId, lastTid) -> lastCommittedTid.set(lastTid));

        System.out.print("  Warming up pipeline");
        warmupPipeline(lastCommittedTid);
        System.out.println(" ready.\n");

        drainBacklog();

        for (int tau : tauValues) {
            for (int alpha : alphaValues) {
                if (tau == 0 && alpha == 0) {
                    results.add(new GridPoint(0, 0, 0.0, 0.0));
                    continue;
                }
                GridPoint point = runSinglePoint(tau, alpha, lastCommittedTid);
                results.add(point);
                System.out.println("  Result: " + point);
                coordinator.clearTransactionInputs();   // ← NEW LINE
                drainBacklog();
            }
        }

        writeCsv(results);
        printSummary(results);
        return results;
    }

    private void waitForPreQueuePlateau() throws InterruptedException {
        long lastSubmitted = coordinator.getNumTIDsSubmitted();
        long stableSince = System.currentTimeMillis();
        long deadline = stableSince + 30_000L;

        System.out.print("  Pre-drain: waiting for pre-queue to plateau");
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(500);
            long now = System.currentTimeMillis();
            long submitted = coordinator.getNumTIDsSubmitted();
            if (submitted > lastSubmitted) {
                lastSubmitted = submitted;
                stableSince = now;
                System.out.print(".");
            } else if (now - stableSince > PRE_QUEUE_PLATEAU_MS) {
                System.out.printf(" plateaued at submitted=%d%n", submitted);
                return;
            }
        }
        System.out.printf(" timed out after 30s, proceeding (submitted=%d)%n",
                coordinator.getNumTIDsSubmitted());
    }

    private boolean drainBacklog() throws InterruptedException {
        waitForPreQueuePlateau();

        final long DRAIN_TARGET_INFLIGHT = 5_000L;
        final long POLL_INTERVAL_MS      = 250L;

        long start    = System.currentTimeMillis();
        long deadline = start + MAX_DRAIN_MS;
        long lastLog  = start;
        boolean cleanDrain = true;

        long initialInflight = coordinator.getTotalInflightLoad();

        if (initialInflight <= DRAIN_TARGET_INFLIGHT) {
            System.out.printf("  Drain: already clear (inflight=%d)", initialInflight);
        } else {
            System.out.printf("  Drain: waiting on backlog (inflight=%d)", initialInflight);

            long lastInflight = initialInflight;
            long stalledSince = start;

            while (System.currentTimeMillis() < deadline) {
                long submitted = coordinator.getNumTIDsSubmitted();
                long committed = coordinator.getNumTIDsCommitted();
                long inflight = coordinator.getTotalInflightLoad();

                if (inflight <= DRAIN_TARGET_INFLIGHT) {
                    double took = (System.currentTimeMillis() - start) / 1000.0;
                    System.out.printf(" cleared in %.1fs (inflight=%d)", took, inflight);
                    break;
                }

                long now = System.currentTimeMillis();

                if (inflight < lastInflight) {
                    lastInflight = inflight;
                    stalledSince = now;
                } else if (now - stalledSince > STALL_THRESHOLD_MS) {
                    System.out.printf(" STALLED at %d (no progress for %ds) — giving up early.",
                            inflight, STALL_THRESHOLD_MS / 1000);
                    cleanDrain = false;
                    break;
                }

                if (now - lastLog >= 5_000) {
                    System.out.printf(" [inflight=%d @ %.1fs]", inflight, (now - start) / 1000.0);
                    lastLog = now;
                }
                Thread.sleep(POLL_INTERVAL_MS);
            }

            if (cleanDrain) {
                long afterLoopInflight = coordinator.getNumTIDsSubmitted()
                        - coordinator.getNumTIDsCommitted();
                if (afterLoopInflight > DRAIN_TARGET_INFLIGHT) {
                    System.out.printf(" TIMEOUT after %.0fs (inflight still %d). Proceeding anyway.",
                            MAX_DRAIN_MS / 1000.0, afterLoopInflight);
                    cleanDrain = false;
                }
            }
        }

        System.out.printf(" settling %ds...", POST_DRAIN_SETTLE_MS / 1000);
        Thread.sleep(POST_DRAIN_SETTLE_MS);

        long finalInflight = coordinator.getNumTIDsSubmitted()
                - coordinator.getNumTIDsCommitted();
        if (finalInflight > DRAIN_TARGET_INFLIGHT) {
            System.out.printf(" inflight=%d after settle — pipeline saturated, continuing.%n",
                    finalInflight);
            cleanDrain = false;
        } else {
            System.out.println(" done.");
        }
        return cleanDrain;
    }

    private GridPoint runSinglePoint(int tau, int alpha,
                                     AtomicLong lastCommittedTid)
            throws InterruptedException {

        System.out.printf("%n--- Running grid point τ=%d α=%d ---%n", tau, alpha);

        long startInflight = coordinator.getNumTIDsSubmitted()
                - coordinator.getNumTIDsCommitted();

        final int effectiveMaxInFlight = MAX_IN_FLIGHT_PER_CLIENT * Math.max(tau, 1);

        if (tau > 0 && startInflight >= effectiveMaxInFlight) {
            System.out.printf("  ABORTED: start inflight=%d >= budget=%d — T-clients " +
                            "would never submit. Marking cell as saturated.%n",
                    startInflight, effectiveMaxInFlight);
            return new GridPoint(tau, alpha, 0.0, 0.0, true);
        }

        if (startInflight > 0) {
            System.out.printf("  WARNING: Start inflight=%d (drain did not fully clear)%n",
                    startInflight);
        }

        AtomicBoolean tRunning = new AtomicBoolean(true);
        ExecutorService tPool = null;
        List<HATtrickTClientWorker> tWorkers = new ArrayList<>();

        if (tau > 0) {
            System.out.printf("  Backpressure budget: %d (=%d × %d T-clients)%n",
                    effectiveMaxInFlight, MAX_IN_FLIGHT_PER_CLIENT, tau);
            tPool = Executors.newFixedThreadPool(tau);
            for (int t = 0; t < tau; t++) {
                HATtrickTClientWorker w = new HATtrickTClientWorker(
                        t, coordinator, tRunning, numWare, effectiveMaxInFlight);
                tWorkers.add(w);
                tPool.submit(w);
            }
        }

        System.out.printf("  Warmup %ds...%n", warmupSecs);
        Thread.sleep(warmupSecs * 1_000L);

        if (tau > 0) {
            long totalSubmittedSoFar = tWorkers.stream()
                    .mapToLong(HATtrickTClientWorker::getSubmittedCount).sum();
            if (totalSubmittedSoFar == 0) {
                System.out.printf("  WARNING: %d T-client(s) submitted 0 tx during warmup. "
                                + "Inflight=%d — backpressure likely stuck.%n",
                        tau, coordinator.getNumTIDsSubmitted() - coordinator.getNumTIDsCommitted());
            }
        }

        long tStart      = lastCommittedTid.get();
        long windowStart = System.currentTimeMillis();

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

        System.out.printf("  Measuring %ds...%n", measurementSecs);
        Thread.sleep(measurementSecs * 1_000L);

        long tEnd      = lastCommittedTid.get();
        long aEnd      = aWorkers.stream()
                .mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
        long windowEnd = System.currentTimeMillis();

        aRunning.set(false);
        if (aPool != null) {
            aPool.shutdownNow();
            aPool.awaitTermination(3, TimeUnit.SECONDS);
        }
        tRunning.set(false);
        if (tPool != null) {
            tPool.shutdownNow();
            tPool.awaitTermination(3, TimeUnit.SECONDS);
        }

        double elapsedSec        = (windowEnd - windowStart) / 1000.0;
        long   committedInWindow = tEnd - tStart;
        double tTps              = committedInWindow / elapsedSec;
        double aQps              = (aEnd - aStart) / elapsedSec;

        long windowSubmitted = tWorkers.stream()
                .mapToLong(HATtrickTClientWorker::getSubmittedCount).sum();

        if (tau > 1) {
            StringBuilder perClient = new StringBuilder("  Per-client submitted: ");
            for (int i = 0; i < tWorkers.size(); i++) {
                perClient.append("c").append(i).append("=")
                        .append(tWorkers.get(i).getSubmittedCount());
                if (i < tWorkers.size() - 1) perClient.append(", ");
            }
            System.out.println(perClient);
        }

        System.out.printf("  τ=%d α=%d  T-tps=%.2f  A-qps=%.4f  "
                        + "(window=%.1fs, submitted=%d, committed=%d)%n",
                tau, alpha, tTps, aQps, elapsedSec, windowSubmitted, committedInWindow);

        if (tau > 0 && windowSubmitted == 0) {
            System.out.printf("  ** INVALID: T-clients submitted 0 tx — point discarded **%n");
        }

        return new GridPoint(tau, alpha, tTps, aQps);
    }

    private void warmupPipeline(AtomicLong lastCommittedTid) throws InterruptedException {
        AtomicBoolean warmupRunning = new AtomicBoolean(true);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        HATtrickTClientWorker warmupWorker = new HATtrickTClientWorker(
                0, coordinator, warmupRunning, numWare, MAX_IN_FLIGHT_PER_CLIENT);
        pool.submit(warmupWorker);

        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(1_000);
            System.out.print(".");
            if (lastCommittedTid.get() > 0) break;
        }

        Thread.sleep(2_000);

        warmupRunning.set(false);
        pool.shutdownNow();
        pool.awaitTermination(3, TimeUnit.SECONDS);

        long prev = lastCommittedTid.get();
        int stableCount = 0;
        while (stableCount < 2) {
            Thread.sleep(1_000);
            long now = lastCommittedTid.get();
            if (now == prev) stableCount++;
            else { stableCount = 0; prev = now; }
        }
    }

    private void writeCsv(List<GridPoint> results) throws IOException {
        String ts = LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String queryLabel = queryPath.replace("/olap/", "").replace(".", "_").replace("/", "_");
        String filename = "hattrick_" + queryLabel + "_" + ts + ".csv";
        try (BufferedWriter w = new BufferedWriter(new FileWriter(filename))) {
            w.write("tau,alpha,t_tps,a_qps,saturated");
            w.newLine();
            for (GridPoint p : results) {
                w.write(String.format(Locale.ROOT, "%d,%d,%.4f,%.4f,%s",
                        p.tau(), p.alpha(), p.tTps(), p.aQps(),
                        p.saturated() ? "true" : "false"));
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