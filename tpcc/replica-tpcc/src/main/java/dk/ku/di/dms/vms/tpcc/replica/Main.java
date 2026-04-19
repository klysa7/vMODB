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
        int numOrderLine = numWare * 3_500_000;

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
            startOlapHttpServer(txManager, repo, numWare);
            return new ReplicaVmsHttpHandler(txManager, repo, numWare);
        });
    }

    /**
     * OLAP HTTP server on port 8096.
     *
     * /chq6 mirrors the Seller VMS pattern exactly:
     *
     *   Seller:
     *     long lastTid = VMS.lastTidFinished();
     *     transactionManager.beginTransaction(lastTid, 0, lastTid, true);
     *     List<OrderEntry> rows = repo.getOrderEntriesBySellerId(sellerId);
     *     // aggregate in Java
     *
     *   Replica:
     *     long lastTid = VMS.lastTidFinished();
     *     txManager.beginTransaction(lastTid, 0, lastTid, true);
     *     for each warehouse: rows += repo.getOrderLinesByWarehouse(w);
     *     float revenue = sum(row.ol_amount)
     *
     * With num_ware=1, the loop runs once and returns all rows.
     * beginTransaction(readOnly=true) gives MVCC snapshot consistency.
     */
    private static void startOlapHttpServer(ITransactionManager txManager,
                                            IOrderLineReplicaRepository repo,
                                            int numWare) {
        try {
            HttpServer httpServer = HttpServer.create(
                    new InetSocketAddress("0.0.0.0", REPLICA_HTTP_PORT), 0);

            // ── GET /chq6 ─────────────────────────────────────────────────────
            // SELECT SUM(ol_amount) FROM order_line
            httpServer.createContext("/chq6", exchange -> {
                try {
                    long lastTid = VMS == null ? 1L : VMS.lastTidFinished();
                    txManager.beginTransaction(lastTid, 0, lastTid, true);
                    float revenue = 0f;
                    for (int w = 1; w <= numWare; w++) {
                        List<OrderLineReplica> rows = repo.getOrderLinesByWarehouse(w);
                        for (OrderLineReplica r : rows) revenue += r.ol_amount;
                    }
                    sendHttp(exchange, 200, "{\"revenue\":" + revenue + "}");
                } catch (Exception e) {
                    LOGGER.log(System.Logger.Level.WARNING, "chq6 error: " + e.getMessage());
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
            throw new RuntimeException("Failed to start replica OLAP HTTP server", e);
        }
    }

    private static void sendHttp(com.sun.net.httpserver.HttpExchange exchange,
                                 int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
    }

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
                            batch.add(new OrderLineReplica(o, d, w, ol, ol, w, 5, 10.0f, "dist-" + d));
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
            LOGGER.log(System.Logger.Level.INFO,
                    "Replica VMS: populated " + total + " rows in "
                            + (System.currentTimeMillis() - startMs) + "ms");
        }
    }
}