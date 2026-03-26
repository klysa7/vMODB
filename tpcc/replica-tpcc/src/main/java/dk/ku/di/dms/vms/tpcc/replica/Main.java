package dk.ku.di.dms.vms.tpcc.replica;

import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;
import dk.ku.di.dms.vms.tpcc.replica.repositories.IOrderLineReplicaRepository;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

/**
 * Replica VMS main class.
 *
 * Two ports:
 *   8004 — vMODB NIO port (coordinator connects here, inventory sends events here)
 *   8096 — standard HTTP server for OLAP queries from the Calcite gateway
 *
 * Why two ports: the vMODB VMS uses a custom NIO protocol on port 8004 that
 * cannot speak standard HTTP. The gateway's java.net.http.HttpClient needs
 * plain HTTP. So we run a separate HttpServer on 8096 for OLAP queries.
 *
 * OLAP endpoint:
 *   GET http://localhost:8096/chq6  →  {"revenue": <sum of ol_amount>}
 *
 * Populate endpoint (called by proxy menu option 1):
 *   PUT http://localhost:8004/populate
 *   Pre-loads ~300K order_line rows so Experiment II starts from the same
 *   baseline as Experiment I.
 */
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    static final int REPLICA_VMS_PORT  = 8004;
    static final int REPLICA_HTTP_PORT = 8096;

    // Held so the HTTP handler and OLAP server can call VMS.lastTidFinished()
    static VmsApplication VMS;

    public static void main(String[] args) throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        VMS = build(prop);
        VMS.start();
    }

    public static VmsApplication build(Properties prop) throws Exception {
        int numWare = Integer.parseInt(prop.getProperty("num_ware", "1"));

        // Buffer sizing: same formula as order VMS
        int numOrders    = numWare * 30_000 + (20_000 * 10);
        int numOrderLine = numOrders * 10;

        prop.setProperty("max_records.order_line",    String.valueOf(numOrderLine));
        prop.setProperty("table.order_line.chaining", "false");
        prop.setProperty("checkpointing",             "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                REPLICA_VMS_PORT,
                new String[]{
                        "dk.ku.di.dms.vms.tpcc.replica",
                        "dk.ku.di.dms.vms.tpcc.common"
                });

        return VmsApplication.build(options, (txManager, repoLookup) -> {
            IOrderLineReplicaRepository repo =
                    (IOrderLineReplicaRepository) repoLookup.apply("order_line");

            // Start the standard HTTP server for OLAP queries
            startOlapHttpServer(txManager, repo);

            return new ReplicaVmsHttpHandler(txManager, repo, numWare);
        });
    }

    /**
     * Starts a standard HttpServer on port 8096 for OLAP queries.
     * This speaks plain HTTP that java.net.http.HttpClient can talk to.
     *
     * GET /chq6   — CH Q6: SUM(ol_amount) over all replica order_line rows
     * GET /status — health check, returns lastTid and row count
     */
    private static void startOlapHttpServer(ITransactionManager txManager,
                                            IOrderLineReplicaRepository repo) {
        try {
            HttpServer httpServer = HttpServer.create(
                    new InetSocketAddress("0.0.0.0", REPLICA_HTTP_PORT), 0);

            httpServer.createContext("/chq6", exchange -> {
                try {
                    // Revenue is tracked incrementally in ReplicaService.REVENUE_BITS.
                    // No MVCC transaction needed — just read the atomic value.
                    // This is O(1) and avoids vMODB's select * bug on composite PK tables.
                    double revenue = ReplicaService.getRevenue();
                    String body = "{\"revenue\":" + revenue + "}";
                    sendHttp(exchange, 200, body);
                } catch (Exception e) {
                    sendHttp(exchange, 500,
                            "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });

            httpServer.createContext("/status", exchange -> {
                long lastTid = VMS == null ? 0 : VMS.lastTidFinished();
                sendHttp(exchange, 200,
                        "{\"lastTid\":" + lastTid
                                + ",\"vmsPort\":" + REPLICA_VMS_PORT
                                + ",\"httpPort\":" + REPLICA_HTTP_PORT + "}");
            });

            httpServer.setExecutor(Executors.newFixedThreadPool(4));
            httpServer.start();
            LOGGER.log(System.Logger.Level.INFO,
                    "Replica OLAP HTTP server started on port " + REPLICA_HTTP_PORT);

        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to start replica OLAP HTTP server on port "
                            + REPLICA_HTTP_PORT, e);
        }
    }

    private static void sendHttp(com.sun.net.httpserver.HttpExchange exchange,
                                 int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type",
                "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    // -------------------------------------------------------------------------
    // vMODB VMS HTTP handler — handles PUT /populate from the proxy
    // -------------------------------------------------------------------------

    static final class ReplicaVmsHttpHandler extends DefaultHttpHandler {

        private final IOrderLineReplicaRepository repository;
        private final int numWare;

        ReplicaVmsHttpHandler(ITransactionManager txManager,
                              IOrderLineReplicaRepository repository,
                              int numWare) {
            super(txManager);
            this.repository = repository;
            this.numWare    = numWare;
        }

        /**
         * PUT /populate
         *
         * Called by the proxy during menu option 1.
         * Pre-loads ~300K order_line rows (same count as the order VMS populate)
         * so that Experiment II OLAP baselines start from the same table size.
         *
         * ol_amount = 10.0f placeholder — the actual values don't matter for
         * measuring query throughput (qps). Only row count matters for scan time.
         */
        @Override
        public void put(String uri, String body) throws Exception {
            if (!uri.startsWith("/populate")) return;

            LOGGER.log(System.Logger.Level.INFO,
                    "Replica VMS: populating (" + numWare + " warehouse(s))...");
            long startMs = System.currentTimeMillis();

            long lastTid = Math.max(1L, VMS.lastTidFinished());
            this.transactionManager.beginTransaction(lastTid, 0, lastTid, false);

            List<OrderLineReplica> batch = new ArrayList<>(5_000);
            int total = 0;

            for (int w = 1; w <= numWare; w++) {
                for (int d = 1; d <= 10; d++) {
                    for (int o = 1; o <= 3_000; o++) {
                        for (int ol = 1; ol <= 10; ol++) {
                            batch.add(new OrderLineReplica(
                                    o, d, w, ol,
                                    ol,          // ol_i_id placeholder
                                    w,           // ol_supply_w_id
                                    5,           // ol_quantity
                                    10.0f,       // ol_amount placeholder
                                    "dist-" + d  // ol_dist_info
                            ));
                        }
                        if (batch.size() >= 5_000) {
                            this.repository.insertAll(batch);
                            total += batch.size();
                            batch.clear();
                        }
                    }
                }
            }
            if (!batch.isEmpty()) {
                this.repository.insertAll(batch);
                total += batch.size();
            }

            long elapsed = System.currentTimeMillis() - startMs;
            LOGGER.log(System.Logger.Level.INFO,
                    "Replica VMS: populated " + total + " rows in " + elapsed + "ms");

            // Seed the revenue counter so /chq6 returns the correct baseline
            // immediately after populate (300K rows × 10.0f = 3,000,000.0)
            float populateRevenue = total * 10.0f;
            long bits = Float.floatToRawIntBits(populateRevenue) & 0xFFFFFFFFL;
            ReplicaService.REVENUE_BITS.set(bits);
            LOGGER.log(System.Logger.Level.INFO,
                    "Replica VMS: seeded revenue counter = " + populateRevenue);
        }
    }
}