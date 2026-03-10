package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class GatewayHttpHandler implements HttpHandler {

    // ── Existing endpoints (unchanged) ───────────────────────────────────────
    static final String PATH_ORDERS = "/olap/orders";
    static final String PATH_COUNT  = "/olap/orders/count";
    static final String PATH_SUM    = "/olap/orders/sum";
    static final String PATH_AVG    = "/olap/orders/avg";

    // ── HATtrick Phase 1 endpoints ────────────────────────────────────────────
    // CA1: COUNT orders — measures New Order freshness
    // CA2: COUNT order_line — fastest-growing table, strongest freshness signal
    // CA3: COUNT history — measures Payment freshness
    //
    // Each query cross-joins the FRESHNESS table twice (one alias per T-client)
    // so the snapshot's txnnum values for each client travel back together with
    // the count in a single consistent read.
    //
    // The WHERE clause pins each alias to its client_id row, turning the
    // cross-join into a point-lookup: exactly 1 row per alias, so the result
    // is always a single row: (count, txnnum_1, txnnum_2).
    static final String PATH_CA1 = "/olap/ca1";
    static final String PATH_CA2 = "/olap/ca2";
    static final String PATH_CA3 = "/olap/ca3";

    // ── Existing SQL (unchanged) ──────────────────────────────────────────────
    static final String SQL_JOIN = """
        SELECT c.c_id, c.c_first, o.o_id
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
    """;

    static final String SQL_COUNT = """
        SELECT c.c_d_id, COUNT(*)
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
        GROUP BY c.c_d_id
    """;

    static final String SQL_SUM = """
        SELECT c.c_d_id, SUM(o.o_ol_cnt)
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
        GROUP BY c.c_d_id
    """;

    static final String SQL_AVG = """
        SELECT c.c_d_id, AVG(o.o_ol_cnt)
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
        GROUP BY c.c_d_id
    """;

    // ── HATtrick SQL ──────────────────────────────────────────────────────────
    // All three queries have the same shape:
    //   SELECT COUNT(*), f1.txnnum, f2.txnnum
    //   FROM <table>, freshness f1, freshness f2
    //   WHERE f1.client_id = 1 AND f2.client_id = 2
    //
    // The FRESHNESS cross-join is the key mechanism:
    //   - Because f1 and f2 are read in the SAME snapshot as the COUNT,
    //     the txnnums are guaranteed to be consistent with the count.
    //   - If txnnum_1 = 50 but T-client 1 has already committed txnnum=53,
    //     the snapshot missed 3 transactions → freshness score > 0.
    //
    // Table references use "order" schema because orders, order_line,
    // history, and freshness all live on the order VMS.
    static final String SQL_CA1 = """
        SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2
        FROM "order".orders, "order".freshness f1, "order".freshness f2
        WHERE f1.client_id = 1 AND f2.client_id = 2
        GROUP BY f1.txnnum, f2.txnnum
    """;

    static final String SQL_CA2 = """
        SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2
        FROM "order".order_line, "order".freshness f1, "order".freshness f2
        WHERE f1.client_id = 1 AND f2.client_id = 2
        GROUP BY f1.txnnum, f2.txnnum
    """;

    static final String SQL_CA3 = """
        SELECT COUNT(*), f1.txnnum AS txnnum_1, f2.txnnum AS txnnum_2
        FROM "order".history, "order".freshness f1, "order".freshness f2
        WHERE f1.client_id = 1 AND f2.client_id = 2
        GROUP BY f1.txnnum, f2.txnnum
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

        String sql = switch (path) {
            // existing
            case PATH_ORDERS -> SQL_JOIN;
            case PATH_COUNT  -> SQL_COUNT;
            case PATH_SUM    -> SQL_SUM;
            case PATH_AVG    -> SQL_AVG;
            // HATtrick
            case PATH_CA1    -> SQL_CA1;
            case PATH_CA2    -> SQL_CA2;
            case PATH_CA3    -> SQL_CA3;
            default          -> null;
        };

        if (sql == null) {
            send(exchange, 404, jsonError(
                    "Unknown endpoint. Available: "
                            + PATH_ORDERS + ", " + PATH_COUNT + ", "
                            + PATH_SUM    + ", " + PATH_AVG   + ", "
                            + PATH_CA1    + ", " + PATH_CA2   + ", " + PATH_CA3));
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