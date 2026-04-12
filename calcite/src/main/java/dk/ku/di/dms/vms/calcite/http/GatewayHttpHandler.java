package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;
import dk.ku.di.dms.vms.modb.common.schema.network.query.LocalJoinSpec;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.END_OF_STREAM_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.QUERY_RESULT_TYPE;

public final class GatewayHttpHandler implements HttpHandler {

    // B23 FIX: static final HttpClient
    private static final HttpClient REPLICA_HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // ─────────────────────────────────────────────────────────────────────────
    // SCAN SHARING — ScanRegistry (Optimization #5)
    //
    // PROBLEM: at α=2, two identical CHQ6 queries arrive within milliseconds.
    // Without sharing, both fire a full VMS scan independently — double the
    // VMS CPU, double the TCP overhead, double the result processing.
    //
    // SOLUTION: if an identical scan (same SQL + same snapshotId) is already
    // in-flight, the second query joins the first query's CompletableFuture
    // instead of starting a new VMS request. When the first scan completes,
    // Java fans the result out to both HTTP response threads simultaneously.
    //
    // KEY = sqlHash + ":" + snapshotId  (Option A — strict correctness)
    //   Same SQL + same snapshotId → same database state → safe to share. ✓
    //   Different snapshotIds → queries execute independently. No staleness. ✓
    //
    // SNAPSHOTID COLLISION RATE:
    //   Coordinator commits batches every ~230ms. Two queries arriving within
    //   the same batch window get the same snapshotId → sharing fires ~90%.
    //   When snapshotIds differ, queries execute independently — correct always.
    //
    // THREAD SAFETY:
    //   putIfAbsent() is atomic. Exactly one thread creates the future (first).
    //   All other threads for the same key wait on future.get(). No deadlock:
    //   thread A executes the scan, thread B waits — they never block each other.
    //
    // Cite: Zukowski et al. 2007 "Cooperative Scans" (VLDB).
    //   QuestDB Discipline 3 — share the physical result, not the physical scan.
    //   Professor's OPT-1 recommendation.
    // ─────────────────────────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, CompletableFuture<String>> scanRegistry =
            new ConcurrentHashMap<>();

    static final String PATH_Q1   = "/olap/q1";
    static final String PATH_CHQ6 = "/olap/chq6";
    static final String PATH_CHQ6_FAST = "/olap/chq6-fast";
    static final String PATH_CHQ4_FAST = "/olap/chq4-fast";
    static final String PATH_CHQ1 = "/olap/chq1";
    static final String PATH_CHQ1_FAST = "/olap/chq1-fast";
    static final String PATH_CHQ4 = "/olap/chq4";
    static final String PATH_CHQ3 = "/olap/chq3";
    static final String PATH_REPLICA_CHQ6 = "/olap/replica/chq6";
    static final String PATH_REPLICA_CHQ1 = "/olap/replica/chq1";
    static final String REPLICA_CHQ6_URL  = "http://localhost:8096/chq6";
    static final String REPLICA_CHQ1_URL  = "http://localhost:8096/chq1";

    static final String SQL_Q1 = """
        SELECT c.c_d_id, COUNT(*)
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
        GROUP BY c.c_d_id
    """;
    static final String SQL_CHQ6 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity BETWEEN 1 AND 100000
    """;
    static final String SQL_CHQ1 = """
        SELECT ol.ol_number,
               SUM(ol.ol_quantity)  AS sum_qty,
               SUM(ol.ol_amount)    AS sum_amount,
               AVG(ol.ol_quantity)  AS avg_qty,
               AVG(ol.ol_amount)    AS avg_amount,
               COUNT(*)             AS count_order
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
        GROUP BY ol.ol_number
    """;
    static final String SQL_CHQ4 = """
        SELECT o.o_ol_cnt, COUNT(*) AS order_count
        FROM "order".orders o
        JOIN "order".order_line ol
          ON ol.ol_o_id = o.o_id
         AND ol.ol_w_id = o.o_w_id
         AND ol.ol_d_id = o.o_d_id
        WHERE o.o_w_id = 1
          AND o.o_entry_d >= '2007-01-02'
          AND o.o_entry_d <  '2030-01-01'
        GROUP BY o.o_ol_cnt
    """;
    static final String SQL_CHQ3 = """
        SELECT ol.ol_o_id, ol.ol_w_id, ol.ol_d_id,
               SUM(ol.ol_amount) AS revenue,
               o.o_entry_d
        FROM warehouse.customer c
        JOIN "order".orders o
          ON c.c_id   = o.o_c_id
         AND c.c_w_id = o.o_w_id
         AND c.c_d_id = o.o_d_id
        JOIN "order".new_orders nord
          ON nord.no_w_id = o.o_w_id
         AND nord.no_d_id = o.o_d_id
         AND nord.no_o_id = o.o_id
        JOIN "order".order_line ol
          ON ol.ol_w_id = o.o_w_id
         AND ol.ol_d_id = o.o_d_id
         AND ol.ol_o_id = o.o_id
        WHERE c.c_w_id = 1
          AND o.o_entry_d > '2007-01-02'
        GROUP BY ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, o.o_entry_d
    """;

    private final OlapGatewayService service;

    public GatewayHttpHandler(OlapGatewayService service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path   = exchange.getRequestURI().getPath();

        if (!"GET".equalsIgnoreCase(method)) {
            send(exchange, 405, jsonError("Method not allowed. Use GET."));
            return;
        }

        if (PATH_REPLICA_CHQ6.equals(path)) { proxyToReplica(exchange, REPLICA_CHQ6_URL); return; }
        if (PATH_REPLICA_CHQ1.equals(path)) { proxyToReplica(exchange, REPLICA_CHQ1_URL); return; }

        if (PATH_CHQ4_FAST.equals(path)) {
            try { send(exchange, 200, executeChq4Direct()); }
            catch (Exception e) { send(exchange, 500, jsonError("CHQ4 local join error: " + e.getMessage())); }
            return;
        }
        if (PATH_CHQ6_FAST.equals(path)) {
            try { send(exchange, 200, executeChq6Direct()); }
            catch (Exception e) { send(exchange, 500, jsonError("CHQ6 direct error: " + e.getMessage())); }
            return;
        }
        if (PATH_CHQ1_FAST.equals(path)) {
            try { send(exchange, 200, executeChq1Direct()); }
            catch (Exception e) { send(exchange, 500, jsonError("CHQ1 direct error: " + e.getMessage())); }
            return;
        }

        String sql = switch (path) {
            case PATH_Q1   -> SQL_Q1;
            case PATH_CHQ6 -> SQL_CHQ6;
            case PATH_CHQ1 -> SQL_CHQ1;
            case PATH_CHQ4 -> SQL_CHQ4;
            case PATH_CHQ3 -> SQL_CHQ3;
            default        -> null;
        };

        if (sql == null) { send(exchange, 404, jsonError("Unknown endpoint.")); return; }

        // ── SCAN SHARING INTERCEPT ────────────────────────────────────────────
        // Key: sqlHash ensures different queries never share.
        //      snapshotId ensures different database states never share.
        long   snapshotId = service.getCurrentSnapshotId();
        String scanKey    = sql.hashCode() + ":" + snapshotId;

        // putIfAbsent is atomic — exactly one thread wins (returns null = first).
        // All others get the existing future (returns non-null = joiner).
        CompletableFuture<String> newFuture = new CompletableFuture<>();
        CompletableFuture<String> existing  = this.scanRegistry.putIfAbsent(scanKey, newFuture);
        boolean isFirst = (existing == null);
        CompletableFuture<String> future = isFirst ? newFuture : existing;

        if (isFirst) {
            // This thread owns the physical VMS scan.
            // On completion, future.complete() unblocks all waiting joiners.
            try {
                String result = service.execute(sql);
                future.complete(result);
                send(exchange, 200, result);
            } catch (Exception e) {
                future.completeExceptionally(e);
                send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
            } finally {
                // Value-checking remove: only removes if this is still the current future.
                this.scanRegistry.remove(scanKey, newFuture);
            }
        } else {
            // This thread joins the existing scan — no VMS request fired.
            // Blocks here until the first thread calls future.complete(result).
            // Both clients receive the same revenue value. Correct: same snapshotId
            // means same committed database state was visible to both queries.
            try {
                System.out.println(">>> [SCAN SHARING] Joined existing scan. key=" + scanKey);
                String result = future.get(30, TimeUnit.SECONDS);
                send(exchange, 200, result);
            } catch (Exception e) {
                send(exchange, 500, jsonError("Scan sharing error: " + e.getMessage()));
            }
        }
    }

    private String executeChq1Direct() throws Exception {
        long startNano = System.nanoTime();
        long[]   sumQty    = new long[16];
        double[] sumAmount = new double[16];
        long[]   count     = new long[16];

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());
            int[]  projectedCols  = new int[]{3, 7, 8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);
            ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE); buf.putInt(0);
            buf.putLong(System.nanoTime());
            buf.putLong(service.getCurrentSnapshotId());
            buf.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);
            byte[] tableBytes = "order_line".getBytes(StandardCharsets.UTF_8);
            buf.putInt(tableBytes.length); buf.put(tableBytes);
            buf.putInt(0); buf.putInt(0);
            buf.putInt(projectionData.length); buf.put(projectionData);
            int endPos = buf.position();
            buf.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buf.position(endPos); buf.flip();
            out.write(buf.array(), 0, buf.limit()); out.flush();
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE) throw new IllegalStateException("Unexpected type: " + type);
                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen]; in.readFully(batchData);
                ByteBuffer batch = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batch.getLong();
                while (batch.remaining() >= 16) {
                    batch.getInt();
                    int olNumber = batch.getInt(); int olQty = batch.getInt(); float olAmt = batch.getFloat();
                    if (olNumber >= 1 && olNumber <= 15) {
                        sumQty[olNumber] += olQty; sumAmount[olNumber] += olAmt; count[olNumber]++;
                    }
                }
            }
        }
        System.out.printf(">>> [CHQ1 DIRECT] Latency: %.2f ms%n", (System.nanoTime() - startNano) / 1_000_000.0);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"ol_number\",\"sum_qty\",\"sum_amount\",\"avg_qty\",\"avg_amount\",\"count_order\"],");
        int groupCount = 0; for (int i = 1; i <= 15; i++) if (count[i] > 0) groupCount++;
        sb.append("\"resultRowCount\":").append(groupCount).append(",\"result\":[");
        boolean first = true;
        for (int i = 1; i <= 15; i++) {
            if (count[i] == 0) continue;
            if (!first) sb.append(",");
            sb.append("{\"ol_number\":").append(i)
                    .append(",\"sum_qty\":").append(sumQty[i])
                    .append(",\"sum_amount\":").append(sumAmount[i])
                    .append(",\"avg_qty\":").append((double) sumQty[i] / count[i])
                    .append(",\"avg_amount\":").append(sumAmount[i] / count[i])
                    .append(",\"count_order\":").append(count[i]).append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    private String executeChq4Direct() throws Exception {
        long snapshotId = service.getCurrentSnapshotId();
        long startNano  = System.nanoTime();
        long epoch2007 = java.time.LocalDate.of(2007, 1, 2).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        long epoch2030 = java.time.LocalDate.of(2030, 1, 1).atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        String buildPredicatesJson = "[" +
                "{\"columnReference\":{\"columnPosition\":2},\"expression\":\"EQUALS\",\"value\":1}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"GREATER_THAN_OR_EQUAL\",\"value\":" + epoch2007 + "}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"LESS_THAN\",\"value\":" + epoch2030 + "}" + "]";
        LocalJoinSpec spec = new LocalJoinSpec("orders", new int[]{0,1,2}, new int[]{0,1,2}, 6, buildPredicatesJson);
        Map<Integer, Long> groups = new LinkedHashMap<>();
        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());
            byte[] tableBytes = "order_line".getBytes(StandardCharsets.UTF_8);
            byte[] routingBytes = spec.toBytes();
            ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE); buf.putInt(0);
            buf.putLong(System.nanoTime()); buf.putLong(snapshotId);
            buf.put(QueryRequestEvent.MODE_LOCAL_JOIN);
            buf.putInt(tableBytes.length); buf.put(tableBytes);
            buf.putInt(0);
            buf.putInt(routingBytes.length); buf.put(routingBytes);
            buf.putInt(0);
            int endPos = buf.position();
            buf.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buf.position(endPos); buf.flip();
            out.write(buf.array(), 0, buf.limit()); out.flush();
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE) throw new IllegalStateException("Unexpected type: " + type);
                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen]; in.readFully(batchData);
                ByteBuffer batchBuf = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batchBuf.getLong();
                while (batchBuf.remaining() >= 16) {
                    batchBuf.getInt();
                    groups.put(batchBuf.getInt(), batchBuf.getLong());
                }
            }
        }
        System.out.printf(">>> [CHQ4 LOCAL JOIN] Groups: %d | Latency: %.2f ms%n", groups.size(), (System.nanoTime() - startNano) / 1_000_000.0);
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"o_ol_cnt\",\"order_count\"],");
        sb.append("\"resultRowCount\":").append(groups.size()).append(",\"result\":[");
        boolean first = true;
        for (Map.Entry<Integer, Long> e : groups.entrySet()) {
            if (!first) sb.append(",");
            sb.append("{\"o_ol_cnt\":").append(e.getKey()).append(",\"order_count\":").append(e.getValue()).append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    private String executeChq6Direct() throws Exception {
        long startNano = System.nanoTime();
        double totalRevenue = 0.0; long rowCount = 0;
        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());
            int[]  projectedCols  = new int[]{8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);
            ByteBuffer buffer = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buffer.position();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE); buffer.putInt(0);
            buffer.putLong(System.nanoTime());
            buffer.putLong(service.getCurrentSnapshotId());
            buffer.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);
            byte[] tableName = "order_line".getBytes(StandardCharsets.UTF_8);
            buffer.putInt(tableName.length); buffer.put(tableName);
            buffer.putInt(0); buffer.putInt(0);
            buffer.putInt(projectionData.length); buffer.put(projectionData);
            int endPos = buffer.position();
            buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buffer.position(endPos); buffer.flip();
            out.write(buffer.array(), 0, buffer.limit()); out.flush();
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE) throw new IllegalStateException("Unknown type: " + type);
                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen]; in.readFully(batchData);
                ByteBuffer batchBuffer = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batchBuffer.getLong();
                while (batchBuffer.hasRemaining()) {
                    batchBuffer.getInt();
                    totalRevenue += batchBuffer.getFloat(); rowCount++;
                }
            }
        }
        System.out.printf(">>> [CHQ6 DIRECT] Rows: %d | Latency: %.2f ms%n", rowCount, (System.nanoTime() - startNano) / 1_000_000.0);
        return "{\"resultColumns\":[\"revenue\"],\"resultRowCount\":1,"
                + "\"result\":[{\"revenue\":" + totalRevenue + "}]}";
    }

    private static void proxyToReplica(HttpExchange exchange, String url) throws IOException {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .GET().build();
            HttpResponse<String> resp = REPLICA_HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
            send(exchange, resp.statusCode(), resp.body());
        } catch (Exception e) {
            send(exchange, 503, jsonError("Replica unavailable (port 8096): " + e.getMessage()));
        }
    }

    private static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static String jsonError(String msg) {
        return "{\"status\":\"error\",\"message\":" + jsonString(msg) + "}";
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\","\\\\").replace("\"","\\\"")
                .replace("\n","\\n").replace("\r","\\r").replace("\t","\\t") + "\"";
    }
}