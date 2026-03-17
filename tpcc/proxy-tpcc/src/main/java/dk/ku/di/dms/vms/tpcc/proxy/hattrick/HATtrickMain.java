package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;

import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/**
 * Entry point for the HATtrick throughput frontier experiment.
 *
 * Call this from the proxy Main.java menu (option "7. HATtrick experiment").
 *
 * The experiment runs the full (τ, α) grid. Default grid:
 *   τ (T-clients): 0, 1, 2, 4
 *   α (A-clients): 0, 1, 2, 4
 *
 * For each grid point it runs:
 *   - warmupSecs seconds of warmup (discarded)
 *   - measurementSecs seconds of measurement (recorded)
 *
 * Output: hattrick_frontier_<timestamp>.csv
 *
 * The CSV has columns: tau, alpha, t_tps, a_qps
 * Plot it in Python to get the throughput frontier.
 *
 * Before running:
 *   1. Make sure all VMSes are populated (option 1 in proxy menu)
 *   2. Make sure the Calcite gateway is running on port 8095
 *   3. The coordinator will be started automatically if not already running
 */
public final class HATtrickMain {

    private static final System.Logger LOG =
            System.getLogger(HATtrickMain.class.getName());

    public static void run(Coordinator existingCoordinator) {
        Properties props = ConfigUtils.loadProperties();
        Scanner scanner = new Scanner(System.in);

        System.out.println("\n=== HATtrick Throughput Frontier Experiment ===");

        // ── Grid parameters ────────────────────────────────────────────────
        System.out.print("T-client counts τ (comma-separated) [default: 0,1,2]: ");
        String tauInput = scanner.nextLine().trim();
        int[] tauValues = parseInts(tauInput, new int[]{0, 1, 2});

        System.out.print("A-client counts α (comma-separated) [default: 0,1,2]: ");
        String alphaInput = scanner.nextLine().trim();
        int[] alphaValues = parseInts(alphaInput, new int[]{0, 1, 2});

        System.out.print("Warmup seconds [default: 5]: ");
        String warmupInput = scanner.nextLine().trim();
        int warmupSecs = warmupInput.isEmpty() ? 5 : Integer.parseInt(warmupInput);

        System.out.print("Measurement seconds [default: 30]: ");
        String measureInput = scanner.nextLine().trim();
        int measurementSecs = measureInput.isEmpty() ? 30 : Integer.parseInt(measureInput);

        String gatewayHost = props.getProperty("gateway_host", "localhost");
        int    gatewayPort = Integer.parseInt(props.getProperty("gateway_port", "8095"));
        String gatewayUrl  = "http://" + gatewayHost + ":" + gatewayPort;

        int numWarehouses = Integer.parseInt(props.getProperty("num_ware", "1"));

        System.out.printf("%nConfiguration:%n");
        System.out.printf("  Gateway:     %s%n", gatewayUrl);
        System.out.printf("  τ values:    %s%n", java.util.Arrays.toString(tauValues));
        System.out.printf("  α values:    %s%n", java.util.Arrays.toString(alphaValues));
        System.out.printf("  Warmup:      %ds%n", warmupSecs);
        System.out.printf("  Measurement: %ds%n", measurementSecs);
        System.out.printf("  Warehouses:  %d%n", numWarehouses);
        System.out.printf("  Total points: %d  (excluding (0,0))%n",
                tauValues.length * alphaValues.length - 1);
        System.out.printf("  Estimated time: ~%d minutes%n",
                (tauValues.length * alphaValues.length * (warmupSecs + measurementSecs + 2)) / 60);

        System.out.print("\nProceed? [y/n]: ");
        if (!scanner.nextLine().trim().equalsIgnoreCase("y")) {
            System.out.println("Experiment cancelled.");
            return;
        }

        // ── Reset and repopulate VMSes ─────────────────────────────────────
        // The coordinator always starts fresh (batch=1, tid=1). If the VMSes
        // are still running from a previous coordinator session their internal
        // batch tracking is at a higher value (e.g. batch=12). The new
        // coordinator sends batch=1 but the VMSes reject it as out-of-order.
        // Resetting clears VMS batch state so the new coordinator and VMSes
        // agree on the starting batch offset.
        System.out.println("Resetting VMS states...");
        DataLoadUtils.cleanup(true);  // true = reset (clears batch state + data)
        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}

        System.out.println("Repopulating VMS states...");
        boolean truncate = Boolean.parseBoolean(props.getProperty("checkpointing_truncate", "true"));
        java.util.concurrent.ForkJoinPool pool2 = java.util.concurrent.ForkJoinPool.commonPool();
        var f1 = pool2.submit(() -> submitPopulate("warehouse", truncate));
        var f2 = pool2.submit(() -> submitPopulate("inventory", truncate));
        var f3 = pool2.submit(() -> submitPopulate("order", truncate));
        try { f1.get(); f2.get(); f3.get(); }
        catch (Exception e) { System.out.println("Warning: populate error: " + e.getMessage()); }
        // ── Load coordinator if needed ─────────────────────────────────────
        Coordinator coordinator = existingCoordinator;
        if (coordinator == null) {
            System.out.println("Loading coordinator...");
            coordinator = ExperimentUtils.loadCoordinator(props);
            // wait for all VMSes to connect
            System.out.print("Waiting for VMSes to connect");
            int numConnected;
            int attempts = 0;
            do {
                try { Thread.sleep(500); } catch (InterruptedException ignored) {}
                numConnected = coordinator.getConnectedVMSs().size();
                System.out.print(".");
                if (++attempts > 60) {
                    System.out.println("\nTimeout waiting for VMSes!");
                    return;
                }
            } while (numConnected < 3);
            System.out.printf("%n3 VMSes connected.%n");
        }

        // ── Run the experiment ─────────────────────────────────────────────
        try {
            HATtrickRunner runner = new HATtrickRunner(
                    coordinator,
                    gatewayUrl,
                    tauValues,
                    alphaValues,
                    warmupSecs,
                    measurementSecs,
                    numWarehouses
            );
            List<HATtrickRunner.GridPoint> results = runner.run();

            System.out.println("\nExperiment complete.");
            System.out.println("Plot the CSV with:");
            System.out.println("  python3 plot_frontier.py hattrick_frontier_<timestamp>.csv");

        } catch (Exception e) {
            System.out.println("Experiment failed: " + e.getMessage());
            e.printStackTrace(System.out);
        }
    }

    private static void submitPopulate(String vms, boolean truncate) {
        String param = truncate ? "populate" : "load";
        MinimalHttpClient client = null;
        try {
            client = DataLoadUtils.obtainHttpClient(vms);
            int status = client.sendRequest("PUT", "", param);
            if (status != 200) System.out.println("Warning: PUT " + vms + " returned " + status);
        } catch (Exception e) {
            System.out.println("Warning: populate " + vms + ": " + e.getMessage());
        } finally {
            if (client != null) DataLoadUtils.returnHttpClient(vms, client);
        }
    }

    private static int[] parseInts(String input, int[] defaultValue) {
        if (input.isEmpty()) return defaultValue;
        try {
            String[] parts = input.split(",");
            int[] result = new int[parts.length];
            for (int i = 0; i < parts.length; i++) {
                result[i] = Integer.parseInt(parts[i].trim());
            }
            return result;
        } catch (NumberFormatException e) {
            System.out.println("Invalid input, using default.");
            return defaultValue;
        }
    }
}