package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class GatewayHttpHandler implements HttpHandler {

    static final String PATH_ORDERS = "/olap/orders";
    static final String PATH_COUNT  = "/olap/orders/count";
    static final String PATH_SUM    = "/olap/orders/sum";
    static final String PATH_AVG    = "/olap/orders/avg";

    // -------------------------------------------------------------------------
    // HATtrick Q1 group — adapted from SSB Q1.1 / Q1.2 / Q1.3
    //
    // SSB Q1 computes SUM(lo_extendedprice * lo_discount) with filters on
    // year, discount range, and quantity range. In TPC-C schema:
    //   ol_amount    ≈  lo_extendedprice * lo_discount  (revenue per order line)
    //   ol_quantity  ≈  lo_quantity                     (the range filter)
    //   ol_w_id = 1  ≈  single-warehouse experiment scope
    //
    // All three queries are single-table scans over order.order_line —
    // no cross-VMS join required, pure aggregation pushed to the order VMS.
    //
    // Q1.1 SSB: year=1993, quantity < 25, discount 1-3
    // Q1.1 TPC-C: ol_quantity < 25  (low-volume order lines)
    //
    // Q1.2 SSB: yearmonth=199401, quantity 26-35, discount 4-6
    // Q1.2 TPC-C: ol_quantity BETWEEN 26 AND 35  (medium-volume order lines)
    //
    // Q1.3 SSB: week=6 of 1994, quantity 36-50, discount 5-7
    // Q1.3 TPC-C: ol_quantity BETWEEN 36 AND 50  (high-volume order lines)
    // -------------------------------------------------------------------------

    static final String PATH_Q1  = "/olap/q1";   // redirect to /count for the throughput frontier
    static final String PATH_Q11 = "/olap/q1.1";
    static final String PATH_Q12 = "/olap/q1.2";
    static final String PATH_Q13 = "/olap/q1.3";

    // Q1 for the throughput frontier experiment — count per district (needs join)
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

    // Q1.1: SUM of revenue for low-quantity order lines (ol_quantity < 25)
    // SSB equivalent: SUM(lo_extendedprice*lo_discount) WHERE year=1993, qty<25, discount 1-3
    // Analytical meaning: total revenue from small orders — reveals order line data volume
    // and tests the aggregation pipeline on the order VMS without a join.
    static final String SQL_Q11 = """
        SELECT SUM(ol.ol_amount)
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
        AND ol.ol_quantity < 25
    """;

    // Q1.2: SUM of revenue for medium-quantity order lines (26 <= ol_quantity <= 35)
    // SSB equivalent: SUM(lo_extendedprice*lo_discount) WHERE yearmonth=199401, qty 26-35, discount 4-6
    // Analytical meaning: revenue from medium-volume orders — different quantity band
    // exercises predicate pushdown to the VMS.
    static final String SQL_Q12 = """
        SELECT SUM(ol.ol_amount)
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
        AND ol.ol_quantity >= 26
        AND ol.ol_quantity <= 35
    """;

    // Q1.3: SUM of revenue for high-quantity order lines (36 <= ol_quantity <= 50)
    // SSB equivalent: SUM(lo_extendedprice*lo_discount) WHERE week=6/1994, qty 36-50, discount 5-7
    // Analytical meaning: revenue from bulk orders — the narrowest filter, smallest result set,
    // but same scan cost as Q1.1 and Q1.2 since there is no index on ol_quantity.
    static final String SQL_Q13 = """
        SELECT SUM(ol.ol_amount)
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
        AND ol.ol_quantity >= 36
        AND ol.ol_quantity <= 50
    """;

    static final String SQL_JOIN = """
        SELECT c.c_id, c.c_last, o.o_id
        FROM warehouse.customer c
        JOIN "order".orders o
        ON c.c_w_id = o.o_w_id
        AND c.c_d_id = o.o_d_id
        AND c.c_id = o.o_c_id
        WHERE c.c_w_id = 1
    """;

    static final String SQL_COUNT = SQL_Q1;

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

    private final OlapGatewayService service;

    public GatewayHttpHandler(OlapGatewayService service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        if (!"GET".equalsIgnoreCase(method)) {
            send(exchange, 405, jsonError("Method not allowed. Use GET."));
            return;
        }

        String sql = switch (path) {
            case PATH_ORDERS -> SQL_JOIN;
            case PATH_COUNT  -> SQL_COUNT;
            case PATH_SUM    -> SQL_SUM;
            case PATH_AVG    -> SQL_AVG;
            case PATH_Q1     -> SQL_Q1;
            case PATH_Q11    -> SQL_Q11;
            case PATH_Q12    -> SQL_Q12;
            case PATH_Q13    -> SQL_Q13;
            default          -> null;
        };

        if (sql == null) {
            send(exchange, 404, jsonError(
                    "Unknown endpoint. Available: "
                            + PATH_ORDERS + ", " + PATH_COUNT + ", "
                            + PATH_Q1  + ", "
                            + PATH_Q11 + ", " + PATH_Q12 + ", " + PATH_Q13 + ", "
                            + PATH_SUM + ", " + PATH_AVG));
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