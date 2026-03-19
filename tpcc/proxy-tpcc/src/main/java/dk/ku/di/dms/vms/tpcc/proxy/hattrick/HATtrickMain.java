package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class HATtrickMain {

    // Coordinator is created once and reused across all phases in the same session.
    // Phase 1 creates it; Phases 2 and 3 reuse it.
    private static Coordinator sharedCoordinator = null;

    // Track whether Phase 1 has run in this session — if yes, warn before Phase 3
    // because Phase 1 adds ~700,000 order_line rows that slow OLAP queries.
    private static boolean phase1WasRun = false;

    @SuppressWarnings("unchecked")
    public static void run(Coordinator existingCoordinator) {
        // If the caller already has a coordinator (from a previous option 4 run),
        // use it. Otherwise we create one lazily in the phase that needs it.
        if (existingCoordinator != null) sharedCoordinator = existingCoordinator;
        Properties props = ConfigUtils.loadProperties();
        Scanner scanner = new Scanner(System.in);

        System.out.println("\n=== HATtrick Experiment ===");
        System.out.println("  Recommended run order on a FRESH system:");
        System.out.println("    1. Populate VMSes (option 1 in main menu)");
        System.out.println("    2. Phase 2 — Pure OLAP  (starts coordinator, no writes)");
        System.out.println("    3. Phase 3 — Full grid  (table still at initial size)");
        System.out.println("    4. Phase 1 — Pure OLTP  (runs last, table size irrelevant)");
        System.out.println("  WARNING: running Phase 1 before Phase 3 adds ~700,000");
        System.out.println("  order_line rows which slows Q1.1/Q1.2/Q1.3 significantly.");
        System.out.println();
        System.out.println("  1. Phase 1 — Pure OLTP  (verify T-tps)");
        System.out.println("  2. Phase 2 — Pure OLAP  (verify A-qps)");
        System.out.println("  3. Phase 3 — Full grid  (frontier)");
        System.out.print("Choose phase: ");
        String phase = scanner.nextLine().trim();

        // ── Shared config ──────────────────────────────────────────────────
        int numWare = Integer.parseInt(props.getProperty("num_ware", "1"));
        String gatewayUrl = "http://"
                + props.getProperty("gateway_host", "localhost")
                + ":" + props.getProperty("gateway_port", "8095");

        // Transaction ratio from app.properties (same as Main.buildTransactionRatioMap)
        Map<String, Integer> txRatioMap = new TreeMap<>();
        addIfNonZero(txRatioMap, "new_order",    props.getProperty("new_order",    "0"));
        addIfNonZero(txRatioMap, "payment",      props.getProperty("payment",      "0"));
        addIfNonZero(txRatioMap, "order_status", props.getProperty("order_status", "0"));
        if (txRatioMap.isEmpty()) {
            System.out.println("ERROR: no transactions in app.properties");
            return;
        }

        Map<String, Integer> numTxInputPerType = new TreeMap<>();
        addIfPresent(numTxInputPerType, "new_order",
                props.getProperty("new_order_input_size", "0"), txRatioMap);
        addIfPresent(numTxInputPerType, "payment",
                props.getProperty("payment_input_size", "0"), txRatioMap);
        addIfPresent(numTxInputPerType, "order_status",
                props.getProperty("order_status_input_size", "0"), txRatioMap);

        Tuple<Integer, String>[] txRatio = new Tuple[txRatioMap.size()];
        int i = 0;
        for (var entry : txRatioMap.entrySet())
            txRatio[i++] = Tuple.of(entry.getValue(), entry.getKey());

        switch (phase) {
            case "1" -> runPhase1(props, scanner,
                    txRatio, txRatioMap, numTxInputPerType, numWare);
            case "2" -> runPhase2(scanner, gatewayUrl, props);
            case "3" -> runPhase3(props, scanner,
                    txRatio, txRatioMap, numTxInputPerType, numWare, gatewayUrl);
            default  -> System.out.println("Invalid choice.");
        }
    }

    // ── Phase 1: Pure OLTP ────────────────────────────────────────────────────
    // Uses ExperimentUtils.runExperiment — the EXACT same path as option 4.
    // Iterates over τ = {1, 2} workers to find X^T.
    // Each run: generate workload → load coordinator → run experiment → print TPS.

    private static void runPhase1(Properties props, Scanner scanner,
                                  Tuple<Integer, String>[] txRatio,
                                  Map<String, Integer> txRatioMap,
                                  Map<String, Integer> numTxInputPerType,
                                  int numWare) {
        System.out.println("\n--- Phase 1: Pure OLTP ---");
        System.out.print("T-client counts τ (comma-separated) [default: 1,2]: ");
        int[] tauValues = parseInts(scanner.nextLine().trim(), new int[]{1, 2});

        System.out.print("Warmup ms [default: 5000]: ");
        String wi = scanner.nextLine().trim();
        int warmupMs = wi.isEmpty() ? 5000 : Integer.parseInt(wi);

        System.out.print("Duration ms [default: 30000]: ");
        String di = scanner.nextLine().trim();
        int durationMs = di.isEmpty() ? 30000 : Integer.parseInt(di);

        // Load coordinator once for all τ runs — stored in sharedCoordinator
        // so Phase 3 can reuse it without hitting "Address already in use".
        sharedCoordinator = loadCoordinator(sharedCoordinator, props);
        if (sharedCoordinator == null) return;

        System.out.println("\nτ values to run: " + Arrays.toString(tauValues));
        System.out.println("Each run: " + warmupMs + "ms warmup + " + durationMs + "ms measurement");
        System.out.print("Proceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;

        for (int tau : tauValues) {
            if (tau == 0) {
                System.out.println("\n  Skipping τ=0 (no T-workers → nothing to measure)");
                continue;
            }
            System.out.printf("%n=== Running Pure OLTP with τ=%d T-workers ===%n", tau);

            // IMPORTANT: always generate files for numWare=1 warehouse only.
            // For τ>1 workers, we duplicate the warehouse-1 input list so all
            // workers submit w_id=1 transactions — the only warehouse with data.
            // Generating files for warehouse_id=2 would cause aborts because
            // only warehouse 1 is populated.
            System.out.printf("  Generating workload for %d warehouse(s)...%n", numWare);
            try {
                WorkloadUtils.createWorkload(numWare, false, numTxInputPerType);
            } catch (IOException e) {
                System.out.println("ERROR generating workload: " + e.getMessage());
                continue;
            }

            // Load 1 set of input iterators for warehouse 1, then duplicate
            // τ times so each worker gets its own independent iterator.
            var baseInput = WorkloadUtils.mapWorkloadInputFiles(numWare, txRatioMap);
            var input = new ArrayList<Map<String, Iterator<Object>>>();
            for (int w = 0; w < tau; w++) {
                // Each worker needs its own fresh iterator set — reload from disk
                try {
                    WorkloadUtils.createWorkload(numWare, false, numTxInputPerType);
                } catch (IOException ignored) {}
                input.addAll(WorkloadUtils.mapWorkloadInputFiles(numWare, txRatioMap));
            }

            ExperimentUtils.ExperimentStats stats = ExperimentUtils.runExperiment(
                    sharedCoordinator, txRatio, input, durationMs, warmupMs);

            System.out.printf("%n  τ=%d  T-tps=%.2f  (committed=%d in %dms)%n",
                    tau, stats.txPerSec(), stats.numCompleted(), durationMs);

            System.out.println("  Draining pipeline (3s)...");
            try { Thread.sleep(3000); } catch (InterruptedException ignored) {}
        }
        System.out.println("\nPhase 1 complete. Note down your X^T values above.");
        phase1WasRun = true;
    }

    // ── Phase 2: Pure OLAP ────────────────────────────────────────────────────
    // Uses HATtrickAClientWorker only. No coordinator needed.
    // Iterates over α = {1, 2} to find X^A.

    private static void runPhase2(Scanner scanner, String gatewayUrl,
                                  Properties props) {
        System.out.println("\n--- Phase 2: Pure OLAP ---");
        System.out.println("  (Coordinator will be started if not already running");
        System.out.println("   so the gateway can fetch the catalog.)");

        // Gateway fetches catalog from coordinator on first query.
        // If coordinator is not running, every query returns HTTP 500.
        sharedCoordinator = loadCoordinator(sharedCoordinator, props);

        System.out.println("  Available queries:");
        System.out.println("    q1   — distributed broadcast join (customer + orders)");
        System.out.println("    q1.1 — SUM(ol_amount) WHERE ol_quantity < 25");
        System.out.println("    q1.2 — SUM(ol_amount) WHERE ol_quantity 26-35");
        System.out.println("    q1.3 — SUM(ol_amount) WHERE ol_quantity 36-50");
        System.out.print("  Query to run [default: q1.1]: ");
        String queryChoice = scanner.nextLine().trim();
        String queryPath = queryChoice.isEmpty() ? "/olap/q1.1" : "/olap/" + queryChoice;

        System.out.print("A-client counts α (comma-separated) [default: 1,2]: ");
        int[] alphaValues = parseInts(scanner.nextLine().trim(), new int[]{1, 2});

        System.out.print("Measurement seconds [default: 30]: ");
        String mi = scanner.nextLine().trim();
        int measurementSecs = mi.isEmpty() ? 30 : Integer.parseInt(mi);

        System.out.println("\nα values to run: " + Arrays.toString(alphaValues));
        System.out.println("Each run: " + measurementSecs + "s measurement");
        System.out.print("Proceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;

        for (int alpha : alphaValues) {
            if (alpha == 0) {
                System.out.println("\n  Skipping α=0 (no A-workers → nothing to measure)");
                continue;
            }
            System.out.printf("%n=== Running Pure OLAP with α=%d A-workers ===%n", alpha);

            var aRunning = new java.util.concurrent.atomic.AtomicBoolean(true);
            var aPool = java.util.concurrent.Executors.newFixedThreadPool(alpha);
            var aWorkers = new ArrayList<HATtrickAClientWorker>();

            for (int i = 0; i < alpha; i++) {
                var w = new HATtrickAClientWorker(i, gatewayUrl, queryPath, aRunning);
                aWorkers.add(w);
                aPool.submit(w);
            }

            try {
                // Warmup: let first query complete before measuring
                // (gateway is cold on first request — takes longer than steady state)
                System.out.println("  Warmup 10s (letting first query complete)...");
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
                try { aPool.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS); }
                catch (InterruptedException ignored) {}
            }
        }
        System.out.println("\nPhase 2 complete. Note down your X^A values above.");
    }

    // ── Phase 3: Full grid ────────────────────────────────────────────────────
    // Only run after phases 1 and 2 are verified.
    // Delegates to HATtrickRunner for the combined grid.

    private static void runPhase3(Properties props, Scanner scanner,
                                  Tuple<Integer, String>[] txRatio,
                                  Map<String, Integer> txRatioMap,
                                  Map<String, Integer> numTxInputPerType,
                                  int numWare, String gatewayUrl) {
        System.out.println("\n--- Phase 3: Full Grid ---");

        System.out.println("  Available queries:");
        System.out.println("    q1   — distributed broadcast join");
        System.out.println("    q1.1 — SUM(ol_amount) WHERE ol_quantity < 25");
        System.out.println("    q1.2 — SUM(ol_amount) WHERE ol_quantity 26-35");
        System.out.println("    q1.3 — SUM(ol_amount) WHERE ol_quantity 36-50");
        System.out.print("  Query to run [default: q1.1]: ");
        String queryChoice = scanner.nextLine().trim();
        String queryPath = queryChoice.isEmpty() ? "/olap/q1.1" : "/olap/" + queryChoice;

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

        // Warn if Phase 1 already ran — order_line table is now much larger
        // than the initial populated state, which will slow Q1.1/Q1.2/Q1.3.
        if (phase1WasRun) {
            System.out.println();
            System.out.println("  *** WARNING ***");
            System.out.println("  Phase 1 ran earlier in this session and added ~700,000");
            System.out.println("  order_line rows. Q1.1/Q1.2/Q1.3 will scan a much larger");
            System.out.println("  table than intended, giving lower A-qps than a fresh run.");
            System.out.println("  For accurate results: restart all VMSes, repopulate,");
            System.out.println("  and run Phase 2 → Phase 3 → Phase 1 in that order.");
            System.out.print("  Continue anyway? [y/n]: ");
            if (!scanner.nextLine().trim().equalsIgnoreCase("y")) return;
        }
        if (sharedCoordinator == null) return;

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

    private static void addIfNonZero(Map<String, Integer> map, String key, String val) {
        int v = Integer.parseInt(val.trim());
        if (v != 0) map.put(key, v);
    }

    private static void addIfPresent(Map<String, Integer> map, String key,
                                     String val, Map<String, Integer> ratioMap) {
        if (ratioMap.containsKey(key)) {
            int v = Integer.parseInt(val.trim());
            if (v > 0) map.put(key, v);
        }
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