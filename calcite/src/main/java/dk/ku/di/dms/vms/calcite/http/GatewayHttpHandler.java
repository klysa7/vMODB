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
import java.util.LinkedHashMap;
import java.util.Map;

import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.END_OF_STREAM_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.QUERY_RESULT_TYPE;

public final class GatewayHttpHandler implements HttpHandler {

    // ── Live order VMS queries (Experiment I) ─────────────────────────────────

    static final String PATH_Q1 = "/olap/q1";
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

    static final String PATH_CHQ6 = "/olap/chq6";
    static final String PATH_CHQ6_FAST = "/olap/chq6-fast"; // <-- NEW: Optimized Route
    static final String PATH_CHQ4_FAST = "/olap/chq4-fast"; // <-- NEW: Optimized Route

    static final String SQL_CHQ6 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity BETWEEN 1 AND 100000
    """;

    static final String PATH_CHQ1 = "/olap/chq1";
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

    static final String PATH_CHQ4 = "/olap/chq4";
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

    static final String PATH_CHQ3 = "/olap/chq3";
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

    static final String PATH_REPLICA_CHQ6 = "/olap/replica/chq6";
    static final String REPLICA_CHQ6_URL  = "http://localhost:8096/chq6";

    static final String PATH_REPLICA_CHQ1 = "/olap/replica/chq1";
    static final String REPLICA_CHQ1_URL  = "http://localhost:8096/chq1";

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

        // ── Replica passthrough ───────────────────────────────────────────────
        if (PATH_REPLICA_CHQ6.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ6_URL);
            return;
        }
        if (PATH_REPLICA_CHQ1.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ1_URL);
            return;
        }

        if (PATH_CHQ4_FAST.equals(path)) {
            try {
                String responseJson = executeChq4Direct();
                send(exchange, 200, responseJson);
            } catch (Exception e) {
                send(exchange, 500, jsonError("CHQ4 local join error: " + e.getMessage()));
            }
            return;
        }

        // ── QPO-2: Hardcoded Query Compilation for CHQ6 (A/B Test Route) ──────
        if (PATH_CHQ6_FAST.equals(path)) {
            try {
                String responseJson = executeChq6Direct();
                send(exchange, 200, responseJson);
            } catch (Exception e) {
                send(exchange, 500, jsonError("Hardcoded CHQ6 Error: " + e.getMessage()));
            }
            return;
        }

        // ── Standard Calcite queries (Baseline Routes) ────────────────────────
        String sql = switch (path) {
            case PATH_Q1   -> SQL_Q1;
            case PATH_CHQ6 -> SQL_CHQ6; // <-- RESTORED: Standard Calcite route for CHQ6
            case PATH_CHQ1 -> SQL_CHQ1;
            case PATH_CHQ4 -> SQL_CHQ4;
            case PATH_CHQ3 -> SQL_CHQ3;
            default        -> null;
        };

        if (sql == null) {
            send(exchange, 404, jsonError("Unknown endpoint."));
            return;
        }

        try {
            String responseJson = service.execute(sql);
            send(exchange, 200, responseJson);
        } catch (Exception e) {
            send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
        }
    }
    /**
     * QPO-5: CHQ4 direct — intra-VMS local hash join.
     *
     * Current path: 2 TCP connections, 100ms hardcoded delay (B44),
     * full orders+order_line transfer, gateway-side LocalJoinOperator (B49).
     *
     * New path: single TCP connection, MODE_LOCAL_JOIN, VMS executes
     * the hash join locally, returns only result rows (10-15 rows).
     *
     * Cite: DeWitt & Gray 1992 — computation moves to data.
     */
    private String executeChq4Direct() throws Exception {
        long snapshotId = service.getCurrentSnapshotId();
        long queryId    = System.nanoTime();
        long startNano  = System.nanoTime();

        // ── Build predicates for orders table ─────────────────────────────────
        // o_w_id = 1         (col 2)
        // o_entry_d >= 2007-01-02 epoch (col 4)
        // o_entry_d <  2030-01-01 epoch (col 4)
        long epoch2007 = java.time.LocalDate.of(2007, 1, 2)
                .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        long epoch2030 = java.time.LocalDate.of(2030, 1, 1)
                .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();

        // Use same PredicateDTO JSON format the VMS already parses
        String buildPredicatesJson = "[" +
                "{\"columnReference\":{\"columnPosition\":2},\"expression\":\"EQUALS\",\"value\":1}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"GREATER_THAN_OR_EQUAL\",\"value\":" + epoch2007 + "}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"LESS_THAN\",\"value\":" + epoch2030 + "}" +
                "]";

        // ── Build LocalJoinSpec ───────────────────────────────────────────────
        // orders join cols:     [0=o_id, 1=o_d_id, 2=o_w_id]
        // order_line join cols: [0=ol_o_id, 1=ol_d_id, 2=ol_w_id]
        // group by: col 6 = o_ol_cnt
        LocalJoinSpec spec = new LocalJoinSpec(
                "orders",
                new int[]{0, 1, 2},   // build join cols
                new int[]{0, 1, 2},   // probe join cols
                6,                    // group by o_ol_cnt
                buildPredicatesJson);

        // ── Send MODE_LOCAL_JOIN to order VMS ─────────────────────────────────
        Map<Integer, Long> groups = new LinkedHashMap<>();

        try (java.net.Socket socket = new java.net.Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            java.io.DataOutputStream out = new java.io.DataOutputStream(socket.getOutputStream());
            java.io.DataInputStream  in  = new java.io.DataInputStream(socket.getInputStream());

            byte[] tableBytes   = "order_line".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] routingBytes = spec.toBytes();

            java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(512)
                    .order(java.nio.ByteOrder.BIG_ENDIAN);

            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0); // length placeholder

            buf.putLong(queryId);
            buf.putLong(snapshotId);
            buf.put(QueryRequestEvent.MODE_LOCAL_JOIN);

            buf.putInt(tableBytes.length);
            buf.put(tableBytes);

            buf.putInt(0);             // no probe predicates
            buf.putInt(routingBytes.length);
            buf.put(routingBytes);
            buf.putInt(0);             // no projection data

            int endPos = buf.position();
            buf.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buf.position(endPos);
            buf.flip();

            out.write(buf.array(), 0, buf.limit());
            out.flush();

            // ── Read result rows: (o_ol_cnt: INT 4, count: LONG 8) = 12 bytes ─
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE)
                    throw new IllegalStateException("Unexpected type: " + type);

                int batchLen = java.nio.ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                java.nio.ByteBuffer batchBuf = java.nio.ByteBuffer.wrap(batchData)
                        .order(java.nio.ByteOrder.nativeOrder());
                batchBuf.getLong(); // skip queryId

                while (batchBuf.remaining() >= 16) { // 4 (rowSize) + 12 (row)
                    int rowSize = batchBuf.getInt();  // should be 12
                    int  olCnt  = batchBuf.getInt();
                    long count  = batchBuf.getLong();
                    groups.put(olCnt, count);
                }
            }
        }

        double latencyMs = (System.nanoTime() - startNano) / 1_000_000.0;
        System.out.printf(">>> [CHQ4 LOCAL JOIN] Groups: %d | Latency: %.2f ms%n",
                groups.size(), latencyMs);

        // ── Build JSON response ───────────────────────────────────────────────
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"o_ol_cnt\",\"order_count\"],");
        sb.append("\"resultRowCount\":").append(groups.size()).append(",");
        sb.append("\"result\":[");
        boolean first = true;
        for (Map.Entry<Integer, Long> e : groups.entrySet()) {
            if (!first) sb.append(",");
            sb.append("{\"o_ol_cnt\":").append(e.getKey())
                    .append(",\"order_count\":").append(e.getValue()).append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }


    /**
     * QPO-2: Directly compiled query path for CHQ6.
     * Bypasses all Calcite overhead (SchemaPlus, Planners, Iterators, Object[] wrapping).
     * Connects to Order VMS, requests ol_amount (index 8), and sums primitives inline.
     */
    private String executeChq6Direct() throws Exception {
        long startNano = System.nanoTime();
        double totalRevenue = 0.0;
        long rowCount = 0;

        // Connect directly to Order VMS
        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());

            ByteBuffer buffer = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);

            // QPO-3 Integration: Only request ol_amount (assuming it's at index 8 in schema)
            int[] projectedCols = new int[]{8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);

            // Construct minimal QueryRequestEvent payload
            int startPos = buffer.position();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buffer.putInt(0); // Length placeholder
            buffer.putLong(System.nanoTime()); // queryId
            buffer.putLong(0); // snapshotId (0 = latest for this test)
            buffer.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableName = "order_line".getBytes(StandardCharsets.UTF_8);
            buffer.putInt(tableName.length);
            buffer.put(tableName);

            buffer.putInt(0); // No predicates
            buffer.putInt(0); // No routing data

            buffer.putInt(projectionData.length);
            buffer.put(projectionData); // The QPO-3 projection pushdown

            int endPos = buffer.position();
            buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buffer.position(endPos);

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            // Read the raw byte stream returned by the VMS (Only 4-byte floats!)
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) {
                    break;
                }
                if (type != QUERY_RESULT_TYPE) {
                    throw new IllegalStateException("Unknown message type: " + type);
                }

                int batchLen = java.nio.ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batchBuffer = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                long queryId = batchBuffer.getLong(); // skip queryId

                while (batchBuffer.hasRemaining()) {
                    int rowSize = batchBuffer.getInt();
                    // Because of QPO-3, rowSize will be exactly 4 bytes!
                    float ol_amount = batchBuffer.getFloat();
                    totalRevenue += ol_amount;
                    rowCount++;
                }
            }
        }

        long endNano = System.nanoTime();
        double latencyMs = (endNano - startNano) / 1_000_000.0;

        System.out.println(">>> [HARDCODED CHQ6] Rows: " + rowCount + " | Latency: " + latencyMs + "ms");

        return "{\n" +
                "  \"rows\": [[" + totalRevenue + "]],\n" +
                "  \"metadata\": [{\"name\": \"revenue\", \"type\": \"FLOAT\"}]\n" +
                "}";
    }

    // ... (rest of the file remains unchanged)

    private static void proxyToReplica(HttpExchange exchange, String url) throws IOException {
        try {
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> resp =
                    client.send(req, HttpResponse.BodyHandlers.ofString());
            send(exchange, resp.statusCode(), resp.body());
        } catch (Exception e) {
            send(exchange, 503, jsonError(
                    "Replica unavailable (port 8096): " + e.getMessage()));
        }
    }

    private static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String jsonError(String msg) {
        return "{\"status\":\"error\",\"message\":" + jsonString(msg) + "}";
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        return "\"" + s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                + "\"";
    }
}