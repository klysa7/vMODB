package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;

import java.util.Properties;

/**
 * Entry point for the HATtrick freshness experiment.
 *
 * ═══════════════════════════════════════════════════════════════════════
 * THE 4 STEPS — WHERE EACH LIVES
 * ═══════════════════════════════════════════════════════════════════════
 *
 * STEP 1 — Start the 3 VMSs (run BEFORE this class, in separate processes)
 *
 *   Each VMS is its own JVM process. Start them in this order:
 *
 *     java -jar warehouse.jar    (port 8001 — reads warehouse_host from config)
 *     java -jar inventory.jar    (port 8002 — reads inventory_host from config)
 *     java -jar order.jar        (port 8003 — reads order_host from config)
 *
 *   application.properties keys needed:
 *     warehouse_host = localhost   (or real IP)
 *     inventory_host = localhost
 *     order_host     = localhost
 *     num_ware       = 1
 *     num_t_clients  = 2
 *
 * ───────────────────────────────────────────────────────────────────────
 *
 * STEP 2 — Load data (HTTP PUT to each VMS — done inside loadData() below)
 *
 *   Fires PUT /warehouse/put, PUT /inventory/put, PUT /order/put.
 *   This populates TPC-C tables AND seeds the FRESHNESS rows in order VMS.
 *   The existing DataLoader class in tpcc.proxy.dataload does this already;
 *   call it before starting the experiment, or integrate it here.
 *
 *   You only need to do this once per VMS restart.
 *
 * ───────────────────────────────────────────────────────────────────────
 *
 * STEP 3 — Build the coordinator (done in ExperimentUtils.loadCoordinator)
 *
 *   The coordinator connects to all 3 VMSs over the network, registers
 *   the transaction DAGs (new_order, payment, order_status), and starts
 *   its own thread. This is unchanged from the existing TPC-C experiment.
 *
 * ───────────────────────────────────────────────────────────────────────
 *
 * STEP 4 — Run the HATtrick experiment (HATtrickRunner, called below)
 *
 *   Launches τ TClientWorkers + α AClientWorkers.
 *   TClientWorkers fire New Order / Payment via coordinator.queueTransactionInput.
 *   AClientWorkers fire CA1/CA2/CA3 COUNT queries to the OLAP gateway HTTP endpoint.
 *   After each experiment point, writes a row to the output CSV.
 *   runQuickGrid() sweeps 3×3 (9 points, ~15 min).
 *   runFullGrid()  sweeps 6×6 (72 points, ~2 hrs).
 *
 * ═══════════════════════════════════════════════════════════════════════
 */
public final class HATtrickMain {

    public static void main(String[] args) throws Exception {

        Properties prop = ConfigUtils.loadProperties();

        int numWare     = Integer.parseInt(prop.getProperty("num_ware",      "1"));
        int numTClients = Integer.parseInt(prop.getProperty("num_t_clients", "2"));

        // The OLAP gateway URL — the HTTP endpoint on the order VMS that
        // accepts a SQL string via POST and returns JSON results.
        // Path is typically /query on the order VMS host.
        String orderHost    = prop.getProperty("order_host", "localhost");
        // Calcite gateway port (default 8095, set GATEWAY_PORT env or gateway_port in properties)
        int gatewayPort = Integer.parseInt(prop.getProperty("gateway_port", "8095"));
        String gatewayHost  = prop.getProperty("gateway_host", "localhost");
        String olapGateway  = "http://" + gatewayHost + ":" + gatewayPort;
        String olapOrderUrl = "http://" + orderHost + ":8003"; // order VMS direct for CA queries

        String outputCsv = prop.getProperty("output_csv", "hattrick_results.csv");

        // ── STEP 2: populate VMS data ──────────────────────────────────────
        // Fires PUT /warehouse/populate, /inventory/populate, /order/populate.
        // Safe to skip if VMSs are already populated (set populate=false in properties).
        boolean populate = Boolean.parseBoolean(prop.getProperty("populate", "true"));
        if (populate) {
            String warehouseHost = prop.getProperty("warehouse_host", "localhost");
            String inventoryHost = prop.getProperty("inventory_host", "localhost");
            System.out.println("Populating VMS data...");
            populateVms("warehouse");
            populateVms("inventory");
            populateVms("order");
            System.out.println("VMS data population complete.");
        } else {
            System.out.println("Skipping data population (populate=false).");
        }

        // ── STEP 3: build coordinator ──────────────────────────────────────
        // ExperimentUtils.loadCoordinator wires the DAGs and connects to VMSs.
        // It also starts the coordinator thread internally.
        Coordinator coordinator = ExperimentUtils.loadCoordinator(prop);

        // Give the coordinator a moment to complete VMS handshakes.
        Thread.sleep(2000);

        // ── STEP 4: wire HATtrick and run ─────────────────────────────────
        // alpha_max / measurement_sec / warmup_sec can be set in application.properties;
        // sensible defaults are provided so the experiment starts without extra config.
        int alphaMax       = Integer.parseInt(prop.getProperty("alpha_max",        "2"));
        int measurementSec = Integer.parseInt(prop.getProperty("measurement_sec",  "60"));
        int warmupSec      = Integer.parseInt(prop.getProperty("warmup_sec",       "10"));

        TransactionCoordinator coord = new VmsTransactionCoordinator(coordinator);

        HATtrickRunner runner = new HATtrickRunner(
                coord,
                olapGateway,
                olapOrderUrl,
                numTClients,   // tauMax  — sweep up to this many T-clients
                alphaMax,      // alphaMax — sweep up to this many A-clients
                numWare,       // numWarehouses
                measurementSec,
                warmupSec,
                outputCsv
        );

        System.out.println("Starting HATtrick experiment. Output: " + outputCsv);
        System.out.println("OLAP gateway: " + olapGateway);
        System.out.println("T-clients: " + numTClients + ", warehouses: " + numWare);

        // Quick grid (3×3 = 9 points) for development / validation
        runner.runQuickGrid();

        // Full grid (6×6 = 72 points) for thesis results — uncomment when ready:
        // runner.runFullGrid();

        System.out.println("Experiment complete. Plot with: python3 plot_results.py " + outputCsv);
        System.exit(0);
    }

    private static void populateVms(String vmsName) {
        dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient client = null;
        try {
            client = dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils.obtainHttpClient(vmsName);
            int status = client.sendRequest("PUT", "", "populate");
            if (status != 200) {
                throw new RuntimeException("Populate failed for " + vmsName + " — HTTP " + status);
            }
            System.out.println("  Populated: " + vmsName + " (HTTP 200)");
        } catch (java.io.IOException e) {
            throw new RuntimeException("Populate error for " + vmsName + ": " + e.getMessage(), e);
        } finally {
            if (client != null)
                dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils.returnHttpClient(vmsName, client);
        }
    }
}