package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

public final class GatewayHttpHandler implements HttpHandler {

    // -------------------------------------------------------------------------
    // Existing cross-VMS join — kept as-is
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // CH-benCHmark Q6 — live order VMS (Experiment I)
    // -------------------------------------------------------------------------
    static final String PATH_CHQ6 = "/olap/chq6";
    static final String SQL_CHQ6 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity BETWEEN 1 AND 100000
    """;

    // -------------------------------------------------------------------------
    // CH-benCHmark Q1
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // CH-benCHmark Q4
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // CH-benCHmark Q3
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Replica CH Q6 — Experiment II
    //
    // Routes the request to the replica VMS (port 8004) instead of going
    // through the Calcite query engine. The replica serves the query directly
    // from its own order_line index via GET /chq6.
    //
    // Why not Calcite here: the replica VMS is not registered in the Calcite
    // catalog (it is not a coordinator-managed VMS for OLAP routing purposes —
    // it only participates as a DAG terminal for OLTP events). A direct HTTP
    // proxy is simpler and avoids catalog registration complexity.
    // -------------------------------------------------------------------------
    static final String PATH_REPLICA_CHQ6 = "/olap/replica/chq6";
    static final String REPLICA_CHQ6_URL  = "http://localhost:8096/chq6";

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

        // ── Replica passthrough (Experiment II) ───────────────────────────
        if (PATH_REPLICA_CHQ6.equals(path)) {
            try {
                HttpClient client = HttpClient.newHttpClient();
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(URI.create(REPLICA_CHQ6_URL))
                        .header("Accept", "application/json")
                        .GET()
                        .build();
                HttpResponse<String> resp =
                        client.send(req, HttpResponse.BodyHandlers.ofString());
                send(exchange, resp.statusCode(), resp.body());
            } catch (Exception e) {
                send(exchange, 503, jsonError(
                        "Replica VMS unavailable (is it running on port 8004?): "
                                + e.getMessage()));
            }
            return;
        }
        // ─────────────────────────────────────────────────────────────────

        String sql = switch (path) {
            case PATH_Q1   -> SQL_Q1;
            case PATH_CHQ6 -> SQL_CHQ6;
            case PATH_CHQ1 -> SQL_CHQ1;
            case PATH_CHQ4 -> SQL_CHQ4;
            case PATH_CHQ3 -> SQL_CHQ3;
            default        -> null;
        };

        if (sql == null) {
            send(exchange, 404, jsonError(
                    "Unknown endpoint. Available: "
                            + PATH_Q1             + " (original cross-VMS join), "
                            + PATH_CHQ6           + " (CH Q6 live order VMS), "
                            + PATH_CHQ1           + " (CH Q1 GROUP BY), "
                            + PATH_CHQ4           + " (CH Q4 JOIN semi-join), "
                            + PATH_CHQ3           + " (CH Q3 cross-VMS join), "
                            + PATH_REPLICA_CHQ6   + " (CH Q6 replica VMS - Experiment II)"
            ));
            return;
        }

        try {
            String responseJson = service.execute(sql);
            send(exchange, 200, responseJson);
        } catch (Exception e) {
            send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
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
        return "{"
                + "\"status\":\"error\","
                + "\"message\":" + jsonString(msg)
                + "}";
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