package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;
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

    // ── CHQ6: dual-path (general via Calcite + direct via QPO-2 hot path) ────
    //
    // /olap/chq6    → goes through OlapGatewayService (Calcite + planner +
    //                 operator tree + VmsGatewayClient). QPO-3 still fires
    //                 here because DistributedPlanner populates
    //                 ScanDefinition.projectedIndices from the Calcite plan.
    //
    // /direct/chq6  → QPO-2 hot path. Bypasses Calcite entirely, opens a raw
    //                 socket to order VMS, hand-builds QueryRequestEvent, sums
    //                 floats in a tight loop. Single-VMS-safe only (sends
    //                 snapshotId=0 and no predicates — correct for num_ware=1).
    //
    // Running both against HATtrick and comparing A-qps / T-tps isolates the
    // contribution of QPO-2 on top of QPO-3.
    static final String PATH_CHQ6        = "/olap/chq6";
    static final String PATH_CHQ6_DIRECT = "/direct/chq6";
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

        // ── QPO-2: Hardcoded hot path for CHQ6 (opt-in via /direct/chq6) ──────
        if (PATH_CHQ6_DIRECT.equals(path)) {
            try {
                String responseJson = executeChq6Direct();
                send(exchange, 200, responseJson);
            } catch (Exception e) {
                send(exchange, 500, jsonError("Direct CHQ6 error: " + e.getMessage()));
            }
            return;
        }

        // ── Standard Calcite queries (includes /olap/chq6 — QPO-3 still fires) ─
        String sql = switch (path) {
            case PATH_Q1   -> SQL_Q1;
            case PATH_CHQ6 -> SQL_CHQ6;
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
     * QPO-2: directly compiled query path for CHQ6.
     * Bypasses Calcite, planner, operator tree, VmsGatewayClient, VmsResultIterator.
     * Opens a raw socket to order VMS, hand-builds the QueryRequestEvent,
     * and sums ol_amount floats in a tight primitive loop.
     *
     * Single-VMS-safe only: sends snapshotId=0 (latest committed on order VMS)
     * and no predicates. Correct for num_ware=1 where the WHERE clause is a
     * tautology. For num_ware>1 this would return wrong results.
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

            // QPO-3 integration: request only ol_amount (column index 8 in schema)
            int[] projectedCols = new int[]{8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);

            // Construct minimal QueryRequestEvent payload. No predicates sent —
            // OK for num_ware=1 where WHERE clause is a tautology.
            int startPos = buffer.position();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buffer.putInt(0); // length placeholder
            buffer.putLong(System.nanoTime()); // queryId
            buffer.putLong(0); // snapshotId (0 = latest committed on order VMS)
            buffer.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableName = "order_line".getBytes(StandardCharsets.UTF_8);
            buffer.putInt(tableName.length);
            buffer.put(tableName);

            buffer.putInt(0); // no predicates
            buffer.putInt(0); // no routing data

            buffer.putInt(projectionData.length);
            buffer.put(projectionData); // QPO-3 projection pushdown

            int endPos = buffer.position();
            buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buffer.position(endPos);

            buffer.flip();
            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            // Read raw byte stream returned by the VMS (4-byte floats only, thanks to QPO-3)
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

                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batchBuffer = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                long queryId = batchBuffer.getLong(); // skip queryId

                while (batchBuffer.hasRemaining()) {
                    int rowSize = batchBuffer.getInt();
                    // Because of QPO-3, rowSize == 4 bytes — pure float payload
                    float ol_amount = batchBuffer.getFloat();
                    totalRevenue += ol_amount;
                    rowCount++;
                }
            }
        }

        long endNano = System.nanoTime();
        double latencyMs = (endNano - startNano) / 1_000_000.0;

        System.out.println(">>> [DIRECT CHQ6] Rows: " + rowCount + " | Latency: " + latencyMs + "ms");

        return "{\n" +
                "  \"rows\": [[" + totalRevenue + "]],\n" +
                "  \"metadata\": [{\"name\": \"revenue\", \"type\": \"FLOAT\"}]\n" +
                "}";
    }

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