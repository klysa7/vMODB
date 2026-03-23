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
 * Replica VMS — two ports:
 *
 *   Port 8004 — vMODB VMS NIO port (coordinator + inventory connect here)
 *   Port 8096 — standard HTTP server for OLAP queries
 *
 * The vMODB VMS on port 8004 uses a custom NIO protocol that cannot speak
 * standard HTTP. The gateway's java.net.http.HttpClient needs standard HTTP.
 * So we run a separate HttpServer on 8096 that answers GET /chq6 using the
 * same transaction manager and repository as the VMS.
 *
 * Gateway routes: /olap/replica/chq6 → http://localhost:8096/chq6
 */
public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    static final int REPLICA_VMS_PORT  = 8004;  // vMODB NIO port
    static final int REPLICA_HTTP_PORT = 8096;  // standard HTTP for OLAP queries

    private static VmsApplication VMS;

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
     * Starts a standard com.sun.net.httpserver.HttpServer on port 8096.
     * This speaks plain HTTP that java.net.http.HttpClient can talk to.
     *
     * Endpoints:
     *   GET /chq6   — CH Q6: SUM(ol_amount) over replica order_line
     *   GET /status — health check
     */
    private static void startOlapHttpServer(ITransactionManager txManager,
                                            IOrderLineReplicaRepository repo) {
        try {
            HttpServer httpServer = HttpServer.create(
                    new InetSocketAddress("0.0.0.0", REPLICA_HTTP_PORT), 0);

            httpServer.createContext("/chq6", exchange -> {
                try {
                    long lastTid = Math.max(1L, VMS.lastTidFinished());
                    txManager.beginTransaction(lastTid, 0, lastTid, true);
                    Chq6Result result = repo.fetchOne(
                            ReplicaService.CHQ6_STMT, Chq6Result.class);
                    double revenue = (result != null) ? result.ol_amount : 0.0;
                    String body = "{\"revenue\":" + revenue + "}";
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type",
                            "application/json; charset=utf-8");
                    exchange.sendResponseHeaders(200, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                } catch (Exception e) {
                    String err = "{\"error\":\"" + e.getMessage() + "\"}";
                    byte[] bytes = err.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(500, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                }
            });

            httpServer.createContext("/status", exchange -> {
                long lastTid = VMS == null ? 0 : VMS.lastTidFinished();
                String body = "{\"lastTid\":" + lastTid
                        + ",\"vmsPort\":" + REPLICA_VMS_PORT
                        + ",\"httpPort\":" + REPLICA_HTTP_PORT + "}";
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type",
                        "application/json; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            });

            httpServer.setExecutor(Executors.newFixedThreadPool(4));
            httpServer.start();
            LOGGER.log(System.Logger.Level.INFO,
                    "Replica OLAP HTTP server started on port " + REPLICA_HTTP_PORT);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start replica OLAP HTTP server", e);
        }
    }

    // -------------------------------------------------------------------------
    // vMODB VMS HTTP handler — handles populate and coordinator handshake
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
                                    o, d, w, ol, ol, w, null, 5, 10.0f, "dist-" + d));
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
        }
    }

    public static final class Chq6Result {
        public double ol_amount;
        public Chq6Result() {}
    }
}