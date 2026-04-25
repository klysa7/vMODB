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
     * OLAP HTTP server on port 8096.
     *
     * Pattern: same as the marketplace Seller VMS — open a read-only MVCC
     * snapshot, scan via repository (no AtomicLong counters), aggregate in
     * Java. Differences vs the original AtomicLong design:
     *
     *   AtomicLong design        Repository scan design (this file)
     *   ─────────────────        ──────────────────────────────────
     *   constant-time query      O(N) scan over order_line
     *   not snapshot-isolated    snapshot at lastTidFinished()
     *   incremental updates      no per-write hot path
     *   ad-hoc to one query      generalizable to any aggregate
     *
     * The scan goes through IRepository.getAll() — which calls
     * TransactionManager.getAll(Table) — bypassing the @Query planner that
     * crashes on composite-PK tables. Predicates (ol_w_id = 1, ol_quantity
     * range) are tautologies for num_ware=1 and applied defensively in the
     * Java aggregation loop.
     *
     * Response envelopes match /olap/chq6 and /olap/chq1 exactly so
     * gateway-level diff against the live paths is byte-comparable.
     */
    private static void startOlapHttpServer(ITransactionManager txManager,
                                            IOrderLineReplicaRepository repo) {
        try {
            HttpServer httpServer = HttpServer.create(
                    new InetSocketAddress("0.0.0.0", REPLICA_HTTP_PORT), 0);

            // ── /chq6 ────────────────────────────────────────────────────
            //
            // SUM(ol_amount) WHERE ol_quantity BETWEEN 1 AND 100000.
            // Predicate is a tautology in standard TPC-C (max ol_quantity
            // is 10) but applied defensively post-scan.
            httpServer.createContext("/chq6", exchange -> {
                try {
                    long startNano = System.nanoTime();
                    long lastTid = VMS == null ? 1L : VMS.lastTidFinished();
                    txManager.beginTransaction(lastTid, 0, lastTid, true);

                    List<OrderLineReplica> rows = repo.getAll();
                    double revenue = 0.0;
                    long kept = 0;
                    for (OrderLineReplica r : rows) {
                        if (r.ol_quantity >= 1 && r.ol_quantity <= 100_000) {
                            revenue += r.ol_amount;
                            kept++;
                        }
                    }

                    double latencyMs = (System.nanoTime() - startNano) / 1_000_000.0;
                    LOGGER.log(System.Logger.Level.INFO,
                            String.format(">>> [REPLICA CHQ6] Scanned: %d | Kept: %d | Revenue: %.4f | Latency: %.2f ms",
                                    rows.size(), kept, revenue, latencyMs));

                    String body = "{\"resultColumns\":[\"revenue\"]," +
                            "\"resultRowCount\":1," +
                            "\"result\":[{\"revenue\":" + revenue + "}]}";
                    sendHttp(exchange, 200, body);
                } catch (Exception e) {
                    LOGGER.log(System.Logger.Level.WARNING, "chq6 error: " + e.getMessage());
                    sendHttp(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });

            // ── /chq1 ────────────────────────────────────────────────────
            //
            // GROUP BY ol_number WHERE ol_w_id = 1, aggregating
            // qty / amount / count. Same MVCC pattern; same primitive-array
            // accumulation as /direct/chq1 (ol_number ∈ [1..15], indexed
            // directly into long[16] / double[16] — no HashMap).
            //
            // ol_w_id = 1 is a tautology for num_ware = 1 but applied
            // defensively post-scan.
            httpServer.createContext("/chq1", exchange -> {
                try {
                    long startNano = System.nanoTime();
                    long lastTid = VMS == null ? 1L : VMS.lastTidFinished();
                    txManager.beginTransaction(lastTid, 0, lastTid, true);

                    List<OrderLineReplica> rows = repo.getAll();

                    long[]   sumQty    = new long[16];
                    double[] sumAmount = new double[16];
                    long[]   count     = new long[16];
                    long kept = 0;

                    for (OrderLineReplica r : rows) {
                        if (r.ol_w_id != 1) continue;
                        int n = r.ol_number;
                        if (n >= 1 && n <= 15) {
                            sumQty[n]    += r.ol_quantity;
                            sumAmount[n] += r.ol_amount;
                            count[n]++;
                            kept++;
                        }
                    }

                    double latencyMs = (System.nanoTime() - startNano) / 1_000_000.0;
                    LOGGER.log(System.Logger.Level.INFO,
                            String.format(">>> [REPLICA CHQ1] Scanned: %d | Kept: %d | Latency: %.2f ms",
                                    rows.size(), kept, latencyMs));

                    int groupCount = 0;
                    for (int i = 1; i <= 15; i++) if (count[i] > 0) groupCount++;

                    StringBuilder sb = new StringBuilder();
                    sb.append("{\"resultColumns\":[\"ol_number\",\"sum_qty\",\"sum_amount\",")
                            .append("\"avg_qty\",\"avg_amount\",\"count_order\"],")
                            .append("\"resultRowCount\":").append(groupCount)
                            .append(",\"result\":[");
                    boolean first = true;
                    for (int i = 1; i <= 15; i++) {
                        if (count[i] == 0) continue;
                        if (!first) sb.append(",");
                        double avgQty    = (double) sumQty[i] / count[i];
                        double avgAmount = sumAmount[i] / count[i];
                        sb.append("{")
                                .append("\"ol_number\":").append(i).append(",")
                                .append("\"sum_qty\":").append(sumQty[i]).append(",")
                                .append("\"sum_amount\":").append(sumAmount[i]).append(",")
                                .append("\"avg_qty\":").append(avgQty).append(",")
                                .append("\"avg_amount\":").append(avgAmount).append(",")
                                .append("\"count_order\":").append(count[i])
                                .append("}");
                        first = false;
                    }
                    sb.append("]}");
                    sendHttp(exchange, 200, sb.toString());
                } catch (Exception e) {
                    LOGGER.log(System.Logger.Level.WARNING, "chq1 error: " + e.getMessage());
                    sendHttp(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
                }
            });

            // ── /status ──────────────────────────────────────────────────
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
                    "Replica OLAP HTTP server started on port " + REPLICA_HTTP_PORT
                            + " (endpoints: /chq6, /chq1, /status)");

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