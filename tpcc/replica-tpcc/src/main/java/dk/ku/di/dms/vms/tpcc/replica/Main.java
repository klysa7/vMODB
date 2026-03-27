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

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    static final int REPLICA_VMS_PORT  = 8004;
    static final int REPLICA_HTTP_PORT = 8096;

    static VmsApplication VMS;

    public static void main(String[] args) throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        VMS = build(prop);
        VMS.start();
    }

    public static VmsApplication build(Properties prop) throws Exception {
        int numWare = Integer.parseInt(prop.getProperty("num_ware", "1"));

        int numOrders    = numWare * 30_000 + (20_000 * 10);
        int numOrderLine = numOrders * 10;

        prop.setProperty("max_records.order_line",    String.valueOf(numOrderLine));
        prop.setProperty("table.order_line.chaining", "false");
        prop.setProperty("checkpointing",             "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop, "0.0.0.0", REPLICA_VMS_PORT,
                new String[]{
                        "dk.ku.di.dms.vms.tpcc.replica",
                        "dk.ku.di.dms.vms.tpcc.common"
                });

        return VmsApplication.build(options, (txManager, repoLookup) -> {
            IOrderLineReplicaRepository repo =
                    (IOrderLineReplicaRepository) repoLookup.apply("order_line");
            startOlapHttpServer(txManager, repo);
            return new ReplicaVmsHttpHandler(txManager, repo, numWare);
        });
    }

    /**
     * Standard HTTP server on port 8096 — speaks plain HTTP for the gateway.
     *
     * Endpoints:
     *   GET /chq6  — total SUM(ol_amount)  → {"revenue": <float>}
     *   GET /chq1  — GROUP BY ol_number    → [{"ol_number":1,"sum_qty":...}, ...]
     *   GET /status — health check
     */
    private static void startOlapHttpServer(ITransactionManager txManager,
                                            IOrderLineReplicaRepository repo) {
        try {
            HttpServer httpServer = HttpServer.create(
                    new InetSocketAddress("0.0.0.0", REPLICA_HTTP_PORT), 0);

            // ── GET /chq6 ────────────────────────────────────────────────────
            httpServer.createContext("/chq6", exchange -> {
                try {
                    double revenue = ReplicaService.getRevenue();
                    sendHttp(exchange, 200, "{\"revenue\":" + revenue + "}");
                } catch (Exception e) {
                    sendHttp(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });

            // ── GET /chq1 ────────────────────────────────────────────────────
            // Returns array of 10 rows grouped by ol_number (1-10).
            // Matches CH Q1 schema: ol_number, sum_qty, sum_amount,
            //                       avg_qty, avg_amount, count_order
            httpServer.createContext("/chq1", exchange -> {
                try {
                    StringBuilder sb = new StringBuilder("[");
                    for (int i = 0; i < 10; i++) {
                        int   olNumber  = i + 1;
                        float sumAmount = ReplicaService.getChq1Amount(i);
                        long  sumQty    = ReplicaService.CHQ1_QUANTITY[i].get();
                        long  count     = ReplicaService.CHQ1_COUNT[i].get();
                        double avgQty    = count > 0 ? (double) sumQty / count : 0.0;
                        double avgAmount = count > 0 ? sumAmount / count : 0.0;
                        if (i > 0) sb.append(",");
                        sb.append("{")
                                .append("\"ol_number\":").append(olNumber).append(",")
                                .append("\"sum_qty\":").append(sumQty).append(",")
                                .append("\"sum_amount\":").append(sumAmount).append(",")
                                .append("\"avg_qty\":").append(String.format("%.4f", avgQty)).append(",")
                                .append("\"avg_amount\":").append(String.format("%.4f", avgAmount)).append(",")
                                .append("\"count_order\":").append(count)
                                .append("}");
                    }
                    sb.append("]");
                    sendHttp(exchange, 200, sb.toString());
                } catch (Exception e) {
                    sendHttp(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });

            // ── GET /status ───────────────────────────────────────────────────
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
                    "Failed to start replica OLAP HTTP server", e);
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

    // ── vMODB VMS HTTP handler — PUT /populate ────────────────────────────────

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
                                    o, d, w, ol, ol, w, 5, 10.0f, "dist-" + d));
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

            // Seed revenue counter: 300K rows × 10.0f = 3,000,000.0
            float populateRevenue = total * 10.0f;
            long bits = Float.floatToRawIntBits(populateRevenue) & 0xFFFFFFFFL;
            ReplicaService.REVENUE_BITS.set(bits);

            // Seed chq1 counters: each ol_number appears total/10 times
            long rowsPerOlNumber = total / 10;
            for (int i = 0; i < 10; i++) {
                // sum_amount = rowsPerOlNumber * 10.0f
                float amt = rowsPerOlNumber * 10.0f;
                ReplicaService.CHQ1_AMOUNT[i].set(
                        Float.floatToRawIntBits(amt) & 0xFFFFFFFFL);
                // sum_qty = rowsPerOlNumber * 5 (ol_quantity=5 in populate)
                ReplicaService.CHQ1_QUANTITY[i].set(rowsPerOlNumber * 5);
                // count = rowsPerOlNumber
                ReplicaService.CHQ1_COUNT[i].set(rowsPerOlNumber);
            }

            LOGGER.log(System.Logger.Level.INFO,
                    "Replica VMS: seeded revenue=" + populateRevenue
                            + " chq1 rows-per-bucket=" + rowsPerOlNumber);
        }
    }
}