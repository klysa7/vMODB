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
    // HATtrick Q1 flight — SSB Q1.1 / Q1.2 / Q1.3 adapted for TPC-C schema
    //
    // SSB Q1 original:
    //   SELECT SUM(lo_extendedprice * lo_discount) as revenue
    //   FROM lineorder, date
    //   WHERE lo_orderdate = d_datekey
    //   WHERE lo_orderdate = d_datekey
    //     AND d_year/d_yearmonthnum/d_weeknuminyear = [VALUE]   -- date filter
    //     AND lo_discount BETWEEN [X] AND [Y]                   -- discount filter
    //     AND lo_quantity [RANGE]                               -- quantity filter
    //
    // TPC-C adaptations:
    //   lo_extendedprice * lo_discount → ol_amount
    //     (ol_amount is the pre-computed line item revenue, equivalent to
    //      lo_extendedprice * lo_discount in SSB)
    //
    //   lo_discount filter → DROPPED
    //     (TPC-C order_line has no discount column; ol_amount already
    //      encodes the final price)
    //
    //   date dimension filter → DROPPED
    //     (TPC-C has no date dimension table; o_entry_d exists on orders
    //      but is populated with current system time at load, so all orders
    //      share the same date — year/month/week filters would return
    //      either all rows or zero rows, making them meaningless)
    //
    //   lo_quantity filter → ol_quantity (direct mapping, same semantics)
    //
    //   lineorder → order_line (ol_w_id = 1 scopes to warehouse 1,
    //     equivalent to SSB SF=1 single-warehouse scope)
    //
    // Filter factors (FF) relative to SSB SF=1 (6,000,000 lineorder rows):
    //   Q1.1 SSB FF = 0.019, rows ≈ 116,883
    //   Q1.1 TPC-C: ol_quantity < 25 filters ~50% of order_line rows
    //     (TPC-C generates qty uniformly in [1..10] per item, most < 25)
    //
    //   Q1.2 SSB FF = 0.00065, rows ≈ 3,896
    //   Q1.2 TPC-C: ol_quantity BETWEEN 26 AND 35 filters a narrower band
    //
    //   Q1.3 SSB FF = 0.000075, rows ≈ 450
    //   Q1.3 TPC-C: ol_quantity BETWEEN 36 AND 50 is the narrowest band
    //
    // The three quantity ranges are disjoint (< 25 / 26-35 / 36-50),
    // matching SSB's intent that queries scan disjoint subsets of the
    // fact table so caching does not interfere between them.
    // -------------------------------------------------------------------------

    static final String PATH_Q1  = "/olap/q1";
    static final String PATH_Q11 = "/olap/q1.1";
    static final String PATH_Q12 = "/olap/q1.2";
    static final String PATH_Q13 = "/olap/q1.3";

    // Q1 — distributed broadcast hash join (cross-VMS)
    // COUNT of orders per district — requires warehouse + order VMS
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

    // Q1.1 — low quantity order lines (ol_quantity < 25)
    // SSB: d_year = 1993, lo_discount BETWEEN 1 AND 3, lo_quantity < 25
    // TPC-C: quantity filter only; date and discount filters dropped (see above)
    static final String SQL_Q11 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity < 25
    """;

    // Q1.2 — medium quantity order lines (26 <= ol_quantity <= 35)
    // SSB: d_yearmonthnum = 199401, lo_discount BETWEEN 4 AND 6, lo_quantity BETWEEN 26 AND 35
    // TPC-C: quantity filter only
    static final String SQL_Q12 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity >= 26
          AND ol.ol_quantity <= 35
    """;

    // Q1.3 — high quantity order lines (36 <= ol_quantity <= 50)
    // SSB: d_weeknuminyear = 6 AND d_year = 1994, lo_discount BETWEEN 5 AND 7,
    //      lo_quantity BETWEEN 36 AND 50 (paper has typo: says 26-35, should be 36-50)
    // TPC-C: quantity filter only
    static final String SQL_Q13 = """
        SELECT SUM(ol.ol_amount) AS revenue
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

    static final String SQL_COUNT = SQL_Q1;  // alias — same as Q1 join query

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