package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HATtrick A-client thread.
 *
 * Issues GET requests to the Calcite OLAP gateway (default port 8095).
 * The gateway parses the SQL via Calcite, sends QueryRequestEvents to the
 * order VMS via VmsGatewayClient, VmsEventHandler executes the scan under
 * MVCC, rows stream back through VmsQueryWorker, and the gateway returns
 * the aggregated result as JSON.
 *
 * Endpoints called:
 *   GET /olap/ca1  → COUNT orders   + freshness txnnums
 *   GET /olap/ca2  → COUNT order_line + freshness txnnums
 *   GET /olap/ca3  → COUNT history  + freshness txnnums
 *
 * Expected response shape (from OlapGatewayService):
 *   {"rows": [[count, txnnum_1, txnnum_2]]}
 *   col 0 = COUNT(*), col 1..n = txnnum per T-client
 */
public final class AClientWorker implements Runnable {

    private static final System.Logger LOG = System.getLogger(AClientWorker.class.getName());
    private static final ObjectMapper  JSON = new ObjectMapper();

    private final int             clientId;
    private final String          gatewayBaseUrl;  // e.g. "http://localhost:8095" (for future OLAP)
    private final String          olapOrderUrl;    // e.g. "http://localhost:8003" (order VMS direct)
    private final int             numTClients;
    private final FreshnessTracker freshnessTracker;
    private final AtomicBoolean   running;

    private final HttpClient http = HttpClient.newHttpClient();

    private volatile long completedQueries = 0L;
    private volatile long startTimeNs      = 0L;
    private boolean loggedFirstResponse    = false;

    public AClientWorker(int clientId,
                         String gatewayBaseUrl,
                         String olapOrderUrl,
                         int numTClients,
                         FreshnessTracker freshnessTracker,
                         AtomicBoolean running) {
        this.clientId         = clientId;
        this.gatewayBaseUrl   = gatewayBaseUrl;
        this.olapOrderUrl     = olapOrderUrl;
        this.numTClients      = numTClients;
        this.freshnessTracker = freshnessTracker;
        this.running          = running;
    }

    @Override
    public void run() {
        startTimeNs = System.nanoTime();
        LOG.log(System.Logger.Level.INFO,
                "A-client {0} started -> {1}/olap/ca[1-3]", clientId, gatewayBaseUrl);

        QueryRecord.QueryType[] types = QueryRecord.QueryType.values();
        int idx = 0;

        while (running.get()) {
            QueryRecord.QueryType qtype = types[idx % types.length];
            idx++;

            String url = endpointFor(qtype);
            long startNs = System.nanoTime();

            try {
                String responseBody = sendGet(url);
                long endNs = System.nanoTime();

                // Log the very first raw response to verify gateway JSON format
                if (!loggedFirstResponse) {
                    loggedFirstResponse = true;
                    LOG.log(System.Logger.Level.INFO,
                            "A-client {0} first gateway response (qtype={1}): {2}",
                            clientId, qtype, responseBody);
                }

                // Empty result — FRESHNESS rows not seeded yet, or gateway
                // still warming up. Retry after a brief pause.
                if (isEmptyResult(responseBody)) {
                    Thread.sleep(50);
                    continue;
                }

                long[] txnnums = parseTxnnums(responseBody);
                long   count   = parseCount(responseBody);

                QueryRecord qr = new QueryRecord(qtype, startNs, endNs, txnnums, count);
                double freshnessScore = freshnessTracker.score(qr);
                completedQueries++;

                if (completedQueries % 100 == 0) {
                    LOG.log(System.Logger.Level.INFO,
                            "A-client {0}: {1} queries, last freshness={2,number,#.###}s",
                            clientId, completedQueries, freshnessScore);
                }

            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING,
                        "A-client {0} query error ({1}): {2}", clientId, qtype, e.getMessage());
                try { Thread.sleep(200); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        LOG.log(System.Logger.Level.INFO,
                "A-client {0} stopped. Completed {1} queries in {2}s  ({3,number,#.##} qps)",
                clientId, completedQueries, elapsedSeconds(), throughputQps());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // HTTP
    // ─────────────────────────────────────────────────────────────────────────

    private String sendGet(String url) throws IOException, InterruptedException {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("Gateway returned HTTP " + resp.statusCode()
                    + ": " + resp.body());
        }
        return resp.body();
    }

    private String endpointFor(QueryRecord.QueryType type) {
        // HATtrick COUNT+freshness queries go directly to the order VMS (port 8003).
        // Reason: Calcite planner has no VModbJoin rule for same-VMS cross-joins,
        // so the gateway cannot yet plan "orders CROSS JOIN freshness".
        // The order VMS HTTP handler executes these in a single MVCC snapshot,
        // giving atomically consistent (count, txnnum_1, txnnum_2).
        return olapOrderUrl + switch (type) {
            case CA1_ORDERS     -> "/query/CA1_ORDERS";
            case CA2_ORDER_LINE -> "/query/CA2_ORDER_LINE";
            case CA3_HISTORY    -> "/query/CA3_HISTORY";
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Parsing — expected: {"rows": [[count, txnnum_1, txnnum_2, ...]]}
    // ─────────────────────────────────────────────────────────────────────────

    // ── Parsing helpers ─────────────────────────────────────────────────────
    // Expected format: {"rows":[[count,txnnum_1,txnnum_2,...]]}
    // The VMS HTTP framework may call toString() on the returned object,
    // producing {rows=[[count, txnnum_1, txnnum_2, ...]]} instead.
    // Both formats are handled below.

    private boolean isEmptyResult(String body) {
        if (body == null || body.isBlank()) return true;
        // Fast path: both JSON and toString() formats contain "rows" followed
        // by "[[" when there is a result row present.
        if (body.contains("rows=[[") || body.contains("\"rows\":[[")) return false;
        try {
            JsonNode root = JSON.readTree(body);
            JsonNode rows = root.path("rows");
            return rows.isMissingNode() || rows.isNull() || rows.isEmpty();
        } catch (Exception e) {
            return true;
        }
    }

    /** Extract the numbers from the first result row as a long[]. */
    private long[] parseRow(String body) {
        // Locate the inner array: the substring between "[[ " and "]]"
        int start = body.indexOf("[[");
        int end   = body.indexOf("]]");
        if (start < 0 || end < 0 || end <= start) return new long[0];
        String inner = body.substring(start + 2, end).trim();
        // Split on ", " or "," handling both JSON and toString spacing
        String[] parts = inner.split("\s*,\s*");
        long[] values = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try { values[i] = Long.parseLong(parts[i].trim()); }
            catch (NumberFormatException ignored) { values[i] = 0L; }
        }
        return values;
    }

    private long[] parseTxnnums(String body) throws IOException {
        long[] row = parseRow(body);
        long[] txnnums = new long[numTClients];
        for (int j = 0; j < numTClients; j++) {
            txnnums[j] = (j + 1 < row.length) ? row[j + 1] : 0L;
        }
        return txnnums;
    }

    private long parseCount(String body) throws IOException {
        long[] row = parseRow(body);
        return row.length > 0 ? row[0] : 0L;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Metrics
    // ─────────────────────────────────────────────────────────────────────────

    public double throughputQps() {
        double e = elapsedSeconds();
        return e > 0 ? completedQueries / e : 0.0;
    }

    public long getCompletedQueries() { return completedQueries; }

    private double elapsedSeconds() {
        return (System.nanoTime() - startTimeNs) / 1e9;
    }
}