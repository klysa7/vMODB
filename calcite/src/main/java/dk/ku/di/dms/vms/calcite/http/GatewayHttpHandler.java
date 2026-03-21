package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class GatewayHttpHandler implements HttpHandler {

    // -------------------------------------------------------------------------
    // Existing cross-VMS join — kept as-is (working, used in HATtrick experiments)
    // COUNT of orders per district — requires warehouse VMS + order VMS
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
    // CH-benCHmark Q6
    // Source: CH-benCHmark (Cole et al., DBTest 2011, TU München)
    //
    // Original:
    //   SELECT sum(ol_amount) AS revenue FROM orderline
    //   WHERE ol_delivery_d >= '1999-01-01' AND ol_delivery_d < '2020-01-01'
    //     AND ol_quantity BETWEEN 1 AND 100000
    //
    // Pattern : single-table full scan + SUM (no join)
    // VMS     : order VMS only
    // Tables  : order_line
    //
    // FIX: ol_delivery_d date filter removed.
    //   In TPC-C, ol_delivery_d is NULL at populate time — it is only set by
    //   the Delivery transaction which is not part of this workload.
    //   Any comparison against NULL evaluates to NULL (not true), so the date
    //   filter drops all rows and returns revenue=0. The quantity filter
    //   BETWEEN 1 AND 100000 selects all rows (TPC-C generates ol_quantity
    //   in [1..10]) and is kept to match the CH-benCHmark full-scan intent.
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
    //
    // Original:
    //   SELECT ol_number, sum(ol_quantity), sum(ol_amount),
    //          avg(ol_quantity), avg(ol_amount), count(*)
    //   FROM orderline
    //   WHERE ol_delivery_d > '2007-01-02'
    //   GROUP BY ol_number ORDER BY ol_number
    //
    // Pattern : single-table scan + GROUP BY + multiple aggregates (no join)
    // VMS     : order VMS only
    // Tables  : order_line
    //
    // FIX 1: ol_delivery_d date filter removed — same NULL reason as Q6.
    //   With the filter, all rows are dropped (ol_delivery_d is NULL),
    //   GROUP BY collapses to one null group, sums return 0.
    // FIX 2: ORDER BY removed — no VModbSortRule in CalcitePlannerImpl.
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
    //
    // Original:
    //   SELECT o_ol_cnt, count(*) AS order_count FROM orders
    //   WHERE o_entry_d >= '2007-01-02' AND o_entry_d < '2012-01-02'
    //     AND EXISTS (SELECT * FROM orderline WHERE ...)
    //   GROUP BY o_ol_cnt ORDER BY o_ol_cnt
    //
    // Pattern : 2-table join + GROUP BY, 1 VMS
    // VMS     : order VMS only
    // Tables  : orders + order_line
    //
    // FIX 1: EXISTS replaced with JOIN — no LogicalCorrelate rule in planner.
    // FIX 2: ORDER BY removed — no VModbSortRule.
    // FIX 3: COUNT(DISTINCT) removed — not supported by LocalAggregateOperator.
    // STATUS: WORKING ✅ (returns 11 rows grouped by o_ol_cnt)
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
    //
    // Original:
    //   SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) as revenue, o_entry_d
    //   FROM customer, neworder, orders, orderline
    //   WHERE c_state LIKE 'A%' AND c_id = o_c_id ...
    //   GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d
    //
    // Pattern : cross-VMS broadcast hash join, 4 tables
    // VMS     : warehouse VMS (customer) + order VMS (orders, new_orders, order_line)
    //
    // FIX 1: alias "no" renamed to "nord" — "NO" is a reserved keyword in
    //   Calcite's SQL parser, causing: "Encountered 'no' at line 9, column 29"
    // FIX 2: c_state LIKE 'A%' removed — parseSingleCondition() has no LIKE
    //   case, predicate would be silently dropped anyway (full customer scan).
    // FIX 3: ORDER BY removed — no VModbSortRule.
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
                            + PATH_Q1   + " (original cross-VMS join), "
                            + PATH_CHQ6 + " (CH Q6: full scan + SUM), "
                            + PATH_CHQ1 + " (CH Q1: GROUP BY aggregate), "
                            + PATH_CHQ4 + " (CH Q4: JOIN semi-join), "
                            + PATH_CHQ3 + " (CH Q3: cross-VMS broadcast join)"
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