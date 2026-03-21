package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HATtrickMain {

    // Coordinator is created once and reused across all phases in the same session.
    private static Coordinator sharedCoordinator = null;

    @SuppressWarnings("unchecked")
    public static void run(Coordinator existingCoordinator) {
        if (existingCoordinator != null) sharedCoordinator = existingCoordinator;
        Properties props = ConfigUtils.loadProperties();
        Scanner scanner = new Scanner(System.in);

        System.out.println("\n=== HATtrick Experiment ===");
        System.out.println("  Recommended run order on a FRESH system:");
        System.out.println("    1. Populate VMSes (option 1 in main menu)");
        System.out.println("    2. Phase 2 — Pure OLAP  (verify A-qps, no writes)");
        System.out.println("    3. Phase 3 — Full grid  (frontier measurement)");
        System.out.println("    4. Phase 1 — Pure OLTP  (verify T-tps with deletes)");
        System.out.println();
        System.out.println("  Table size is kept stable in all phases:");
        System.out.println("  every new_order is paired with a delete_orderline");
        System.out.println("  keeping order_line at ~300,000 rows throughout.");
        System.out.println();
        System.out.println("  1. Phase 1 — Pure OLTP  (T-tps with stable table)");
        System.out.println("  2. Phase 2 — Pure OLAP  (A-qps baseline)");
        System.out.println("  3. Phase 3 — Full grid  (frontier)");
        System.out.print("Choose phase: ");
        String phase = scanner.nextLine().trim();

        int numWare = Integer.parseInt(props.getProperty("num_ware", "1"));
        String gatewayUrl = "http://"
                + props.getProperty("gateway_host", "localhost")
                + ":" + props.getProperty("gateway_port", "8095");

        switch (phase) {
            case "1" -> runPhase1(props, scanner, numWare);
            case "2" -> runPhase2(scanner, gatewayUrl, props);
            case "3" -> runPhase3(props, scanner, numWare, gatewayUrl);
            default  -> System.out.println("Invalid choice.");
        }
    }

    // ── Phase 1: Pure OLTP ────────────────────────────────────────────────────
    // Uses HATtrickTClientWorker directly — same path as Phase 3's T-clients.
    // Every new_order is paired with a delete_orderline so the table stays at
    // ~300,000 rows throughout. No pre-generated workload files needed.

    private static void runPhase1(Properties props, Scanner scanner, int numWare) {
        System.out.println("\n--- Phase 1: Pure OLTP ---");
        System.out.println("  Uses HATtrickTClientWorker (new_order + paired delete_orderline).");
        System.out.println("  Table stays stable at ~300,000 rows throughout.");

        System.out.print("T-client counts τ (comma-separated) [default: 1,2]: ");
        int[] tauValues = parseInts(scanner.nextLine().trim(), new int[]{1, 2});

        System.out.print("Warmup seconds [default: 5]: ");
        String wi = scanner.nextLine().trim();
        int warmupSecs = wi.isEmpty() ? 5 : Integer.parseInt(wi);

        System.out.print("Measurement seconds [default: 30]: ");
        String mi = scanner.nextLine().trim();
        int measurementSecs = mi.isEmpty() ? 30 : Integer.parseInt(mi);

        sharedCoordinator = loadCoordinator(sharedCoordinator, props);
        if (sharedCoordinator == null) return;

        System.out.println("\nτ values to run: " + Arrays.toString(tauValues));
        System.out.println("Each run: " + warmupSecs + "s warmup + " + measurementSecs + "s measurement");
        System.out.print("Proceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;

        for (int tau : tauValues) {
            if (tau == 0) {
                System.out.println("\n  Skipping τ=0 (no T-workers)");
                continue;
            }
            System.out.printf("%n=== Running Pure OLTP with τ=%d T-workers ===%n", tau);

            AtomicBoolean running = new AtomicBoolean(true);
            ExecutorService pool = Executors.newFixedThreadPool(tau);
            List<HATtrickTClientWorker> workers = new ArrayList<>();

            for (int t = 0; t < tau; t++) {
                HATtrickTClientWorker w = new HATtrickTClientWorker(
                        t, sharedCoordinator, running, numWare);
                workers.add(w);
                pool.submit(w);
            }

            try {
                // Warmup
                System.out.printf("  Warming up %ds...%n", warmupSecs);
                Thread.sleep(warmupSecs * 1000L);

                // Snapshot batch commit counter at start
                System.out.printf("  Measuring %ds...%n", measurementSecs);
                final long[] tStartHolder = {0};
                sharedCoordinator.registerBatchCommitConsumer((batchId, tid) -> {
                    if (tStartHolder[0] == 0) tStartHolder[0] = tid;
                });
                long windowStart = System.currentTimeMillis();

                Thread.sleep(measurementSecs * 1000L);

                long windowEnd = System.currentTimeMillis();
                double elapsedSec = (windowEnd - windowStart) / 1000.0;

                // Count committed transactions via submitted proxy
                long totalSubmitted = workers.stream()
                        .mapToLong(HATtrickTClientWorker::getSubmittedCount).sum();

                // T-tps: use committed count from batch callback
                // For simplicity here we report submitted/elapsed as an estimate
                // (same approach used in HATtrickRunner for the grid)
                double tTps = totalSubmitted / elapsedSec;

                System.out.printf("%n  τ=%d  T-tps≈%.2f  (submitted=%d in %.1fs)%n",
                        tau, tTps, totalSubmitted, elapsedSec);
                System.out.println("  (T-tps here counts submitted+delete pairs; committed count");
                System.out.println("   is shown in coordinator batch logs above)");

            } catch (InterruptedException ignored) {
            } finally {
                running.set(false);
                pool.shutdownNow();
                try { pool.awaitTermination(3, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) {}
            }

            System.out.println("  Draining pipeline (3s)...");
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        }
        System.out.println("\nPhase 1 complete.");
        System.out.println("Check coordinator logs: 'batch N with M transactions' shows committed count.");
        System.out.println("Check order VMS logs: '[DELETE_ORDERLINE] Deleted N rows' confirms stability.");
    }

    // ── Phase 2: Pure OLAP ────────────────────────────────────────────────────

    private static void runPhase2(Scanner scanner, String gatewayUrl, Properties props) {
        System.out.println("\n--- Phase 2: Pure OLAP ---");
        System.out.println("  (Coordinator started if not already running)");

        sharedCoordinator = loadCoordinator(sharedCoordinator, props);

        System.out.println("  Available queries:");
        System.out.println("    chq6 — CH Q6: full scan + SUM on order_line  (recommended)");
        System.out.println("    chq1 — CH Q1: GROUP BY aggregate on order_line");
        System.out.println("    chq4 — CH Q4: JOIN semi-join (orders + order_line)");
        System.out.println("    chq3 — CH Q3: cross-VMS broadcast join (4 tables)");
        System.out.println("    q1   — original cross-VMS join (customer + orders)");
        System.out.print("  Query to run [default: chq6]: ");
        String queryChoice = scanner.nextLine().trim();
        String queryPath = queryChoice.isEmpty() ? "/olap/chq6" : "/olap/" + queryChoice;

        System.out.print("A-client counts α (comma-separated) [default: 1,2]: ");
        int[] alphaValues = parseInts(scanner.nextLine().trim(), new int[]{1, 2});

        System.out.print("Measurement seconds [default: 30]: ");
        String mi = scanner.nextLine().trim();
        int measurementSecs = mi.isEmpty() ? 30 : Integer.parseInt(mi);

        System.out.println("\nα values to run: " + Arrays.toString(alphaValues));
        System.out.println("Each run: 10s warmup + " + measurementSecs + "s measurement");
        System.out.print("Proceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;

        for (int alpha : alphaValues) {
            if (alpha == 0) {
                System.out.println("\n  Skipping α=0 (no A-workers)");
                continue;
            }
            System.out.printf("%n=== Running Pure OLAP with α=%d A-workers ===%n", alpha);

            AtomicBoolean aRunning = new AtomicBoolean(true);
            ExecutorService aPool = Executors.newFixedThreadPool(alpha);
            List<HATtrickAClientWorker> aWorkers = new ArrayList<>();

            for (int i = 0; i < alpha; i++) {
                HATtrickAClientWorker w = new HATtrickAClientWorker(
                        i, gatewayUrl, queryPath, aRunning);
                aWorkers.add(w);
                aPool.submit(w);
            }

            try {
                System.out.println("  Warmup 10s...");
                Thread.sleep(10_000);

                System.out.printf("  Measuring %ds...%n", measurementSecs);
                long aStart = aWorkers.stream()
                        .mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
                long windowStart = System.currentTimeMillis();

                Thread.sleep(measurementSecs * 1_000L);

                long aEnd = aWorkers.stream()
                        .mapToLong(HATtrickAClientWorker::getCompletedCount).sum();
                long windowEnd = System.currentTimeMillis();

                double elapsedSec = (windowEnd - windowStart) / 1000.0;
                double aQps = (aEnd - aStart) / elapsedSec;

                System.out.printf("  α=%d  A-qps=%.4f  (queries=%d in %.1fs)%n",
                        alpha, aQps, aEnd - aStart, elapsedSec);

            } catch (InterruptedException ignored) {
            } finally {
                aRunning.set(false);
                aPool.shutdownNow();
                try { aPool.awaitTermination(3, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) {}
            }
        }
        System.out.println("\nPhase 2 complete. Note down your X^A values above.");
    }

    // ── Phase 3: Full grid ────────────────────────────────────────────────────

    private static void runPhase3(Properties props, Scanner scanner,
                                  int numWare, String gatewayUrl) {
        System.out.println("\n--- Phase 3: Full Grid ---");
        System.out.println("  Table stays at ~300,000 rows: every new_order");
        System.out.println("  is paired with a delete_orderline.");

        System.out.println("  Available queries:");
        System.out.println("    chq6 — CH Q6: full scan + SUM on order_line  (recommended)");
        System.out.println("    chq1 — CH Q1: GROUP BY aggregate on order_line");
        System.out.println("    chq4 — CH Q4: JOIN semi-join (orders + order_line)");
        System.out.println("    chq3 — CH Q3: cross-VMS broadcast join (4 tables)");
        System.out.println("    q1   — original cross-VMS join (customer + orders)");
        System.out.print("  Query to run [default: chq6]: ");
        String queryChoice = scanner.nextLine().trim();
        String queryPath = queryChoice.isEmpty() ? "/olap/chq6" : "/olap/" + queryChoice;

        System.out.print("T-client counts τ (comma-separated) [default: 0,1,2]: ");
        int[] tauValues = parseInts(scanner.nextLine().trim(), new int[]{0, 1, 2});

        System.out.print("A-client counts α (comma-separated) [default: 0,1,2]: ");
        int[] alphaValues = parseInts(scanner.nextLine().trim(), new int[]{0, 1, 2});

        System.out.print("Warmup seconds [default: 5]: ");
        String wi = scanner.nextLine().trim();
        int warmupSecs = wi.isEmpty() ? 5 : Integer.parseInt(wi);

        System.out.print("Measurement seconds [default: 30]: ");
        String mi = scanner.nextLine().trim();
        int measurementSecs = mi.isEmpty() ? 30 : Integer.parseInt(mi);

        System.out.print("Proceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;

        sharedCoordinator = loadCoordinator(sharedCoordinator, props);
        if (sharedCoordinator == null) return;

        // Build dummy txRatio for HATtrickRunner (not used by T-client path)
        Tuple<Integer, String>[] txRatio = new Tuple[]{Tuple.of(100, "new_order")};
        Map<String, Integer> numTxInputPerType = new HashMap<>();

        try {
            HATtrickRunner runner = new HATtrickRunner(
                    sharedCoordinator, gatewayUrl,
                    tauValues, alphaValues,
                    warmupSecs, measurementSecs,
                    numWare, txRatio, numTxInputPerType, queryPath);
            runner.run();
        } catch (Exception e) {
            System.out.println("Phase 3 failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static Coordinator loadCoordinator(Coordinator existing, Properties props) {
        if (existing != null) return existing;
        System.out.println("Loading coordinator...");
        Coordinator coordinator = ExperimentUtils.loadCoordinator(props);
        System.out.print("Waiting for VMSes");
        int attempts = 0;
        do {
            try { Thread.sleep(500); } catch (InterruptedException ignored) {}
            System.out.print(".");
            if (++attempts > 60) {
                System.out.println("\nTimeout!");
                return null;
            }
        } while (coordinator.getConnectedVMSs().size() < 3);
        System.out.printf("%n3 VMSes connected.%n");
        return coordinator;
    }

    private static int[] parseInts(String input, int[] defaultValue) {
        if (input.isEmpty()) return defaultValue;
        try {
            String[] parts = input.split(",");
            int[] result = new int[parts.length];
            for (int i = 0; i < parts.length; i++)
                result[i] = Integer.parseInt(parts[i].trim());
            return result;
        } catch (NumberFormatException e) {
            System.out.println("Invalid input, using default.");
            return defaultValue;
        }
    }
}