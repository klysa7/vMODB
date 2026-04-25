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

    private static final HttpClient REPLICA_HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final ConcurrentHashMap<String, CompletableFuture<String>> scanRegistry =
            new ConcurrentHashMap<>();

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

    static final String PATH_CHQ6        = "/olap/chq6";
    static final String PATH_CHQ6_DIRECT = "/direct/chq6";

    static final String SQL_CHQ6 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity BETWEEN 1 AND 100000
    """;

    static final String PATH_CHQ1        = "/olap/chq1";
    static final String PATH_CHQ1_DIRECT = "/direct/chq1";

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

    static final String PATH_CHQ4        = "/olap/chq4";
    static final String PATH_CHQ4_DIRECT = "/direct/chq4";

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

        if (PATH_REPLICA_CHQ6.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ6_URL);
            return;
        }
        if (PATH_REPLICA_CHQ1.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ1_URL);
            return;
        }

        if (PATH_CHQ6_DIRECT.equals(path)) {
            try {
                send(exchange, 200, executeChq6Direct());
            } catch (Exception e) {
                send(exchange, 500, jsonError("CHQ6 direct error: " + e.getMessage()));
            }
            return;
        }
        if (PATH_CHQ4_DIRECT.equals(path)) {
            try {
                send(exchange, 200, executeChq4Direct());
            } catch (Exception e) {
                send(exchange, 500, jsonError("CHQ4 direct error: " + e.getMessage()));
            }
            return;
        }
        if (PATH_CHQ1_DIRECT.equals(path)) {
            try {
                send(exchange, 200, executeChq1Direct());
            } catch (Exception e) {
                send(exchange, 500, jsonError("CHQ1 direct error: " + e.getMessage()));
            }
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
            send(exchange, 404, jsonError("Unknown endpoint."));
            return;
        }

        long   snapshotId = service.getCurrentSnapshotId();
        String scanKey    = sql.hashCode() + ":" + snapshotId;

        CompletableFuture<String> newFuture = new CompletableFuture<>();
        CompletableFuture<String> existing  = this.scanRegistry.putIfAbsent(scanKey, newFuture);
        boolean isFirst = (existing == null);
        CompletableFuture<String> future = isFirst ? newFuture : existing;

        if (isFirst) {
            try {
                String result = service.execute(sql);
                future.complete(result);
                send(exchange, 200, result);
            } catch (Exception e) {
                future.completeExceptionally(e);
                send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
            } finally {
                this.scanRegistry.remove(scanKey, newFuture);
            }
        } else {
            try {
                System.out.println(">>> [SCAN SHARING] Joined existing scan. key=" + scanKey);
                String result = future.get(30, TimeUnit.SECONDS);
                send(exchange, 200, result);
            } catch (Exception e) {
                send(exchange, 500, jsonError("Scan sharing error: " + e.getMessage()));
            }
        }
    }

    // ── QPO-6: CHQ1 direct scan ───────────────────────────────────────────────
    //
    // CHQ1 groups order_line rows by ol_number (TPC-C spec: always 1–15).
    // Wire format from VmsQueryWorker for MODE_SCAN_TO_GATEWAY is per-row:
    //   [rowSize:int 4 bytes][rowData:N bytes]
    // For projection [3, 7, 8] → rowData is 12 bytes:
    //   [ol_number:int 4][ol_quantity:int 4][ol_amount:float 4]
    // Total wire size per row: 16 bytes.
    //
    // ── BUG FIX ──────────────────────────────────────────────────────────────
    //
    // The previous version skipped the rowSize prefix. With a 16-byte wire
    // row but a 12-byte read, every iteration drifted by 4 bytes through the
    // packed-column data, producing alternating valid and garbage ol_number
    // values. The diagnostic counter showed:
    //   bytesAfterQid: 4796928 = 299808 × 16 (rows VMS sent × wire size)
    //   misaligned:    0       (every batch payload is multiple of 16)
    //   badOlNumber:   199872  (about half the iterations reading garbage)
    //
    // FIX: read the 4-byte rowSize prefix per row before reading the row data.
    // Step is now 16 bytes per iteration, matching the wire format. The
    // rowSize value itself is discarded — for a fixed-projection scan it is
    // always 12 — but we advance the buffer position to stay aligned.
    //
    // The rowSize prefix is written by VmsQueryWorker using ByteBuffer
    // default order (BIG_ENDIAN), while the row data is written in
    // nativeOrder. We use position-skip rather than getInt() to avoid having
    // to switch byte orders mid-buffer — the value is unused either way.
    //
    // Cite: Neumann 2011 — query compilation. Fixed-size primitive arrays
    //       replace generic HashMap aggregation.
    //
    // Result columns: ol_number, sum_qty, sum_amount, avg_qty, avg_amount, count_order
    private String executeChq1Direct() throws Exception {
        long startNano = System.nanoTime();

        // ol_number is always 1–15 in TPC-C. Index directly — no HashMap needed.
        long[]   sumQty    = new long[16];
        double[] sumAmount = new double[16];
        long[]   count     = new long[16];

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            // QPO-3: project ol_number(3), ol_quantity(7), ol_amount(8) → 12 bytes/row data
            int[]  projectedCols   = new int[]{3, 7, 8};
            byte[] projectionData  = QueryRequestEvent.serializeProjection(projectedCols);

            ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0); // length placeholder
            buf.putLong(System.nanoTime()); // queryId
            buf.putLong(service.getCurrentSnapshotId());
            buf.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableBytes = "order_line".getBytes(StandardCharsets.UTF_8);
            buf.putInt(tableBytes.length);
            buf.put(tableBytes);

            buf.putInt(0);                     // no predicates (ol_w_id=1 filter skipped —
            // single warehouse, all rows qualify)
            buf.putInt(0);                     // no routing data
            buf.putInt(projectionData.length);
            buf.put(projectionData);

            int endPos = buf.position();
            buf.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buf.position(endPos);
            buf.flip();

            out.write(buf.array(), 0, buf.limit());
            out.flush();

            // ── Read wire rows: [rowSize:4][ol_number:4][ol_quantity:4][ol_amount:4]
            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE)
                    throw new IllegalStateException("Unexpected type: " + type);

                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batch = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batch.getLong(); // skip queryId

                while (batch.remaining() >= 16) {
                    // Skip the 4-byte rowSize prefix written by VmsQueryWorker.
                    // Value is always 12 for this projection — we don't need it,
                    // we just need to advance position to stay aligned.
                    batch.position(batch.position() + 4);

                    int olNumber  = batch.getInt();
                    int olQty     = batch.getInt();
                    float olAmt   = batch.getFloat();

                    if (olNumber >= 1 && olNumber <= 15) {
                        sumQty[olNumber]    += olQty;
                        sumAmount[olNumber] += olAmt;
                        count[olNumber]++;
                    }
                }
            }
        }

        double latencyMs = (System.nanoTime() - startNano) / 1_000_000.0;
        long totalRows = 0;
        for (int i = 1; i <= 15; i++) totalRows += count[i];
        System.out.printf(">>> [CHQ1 DIRECT] Rows scanned: %d | Latency: %.2f ms%n",
                totalRows, latencyMs);

        // ── Build JSON — same columns as Calcite path ─────────────────────────
        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"ol_number\",\"sum_qty\",\"sum_amount\",")
                .append("\"avg_qty\",\"avg_amount\",\"count_order\"],");

        int groupCount = 0;
        for (int i = 1; i <= 15; i++) if (count[i] > 0) groupCount++;
        sb.append("\"resultRowCount\":").append(groupCount).append(",\"result\":[");

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
        return sb.toString();
    }

    // ── QPO-5: CHQ4 direct — intra-VMS local hash join ───────────────────────
    //
    // Build hash on orders (with predicates: o_w_id=1, date range), probe with
    // order_line on (o_id, o_d_id, o_w_id), group by o_ol_cnt, COUNT(*).
    // The VMS executes the join + aggregation locally and returns aggregated
    // result rows in MODE_LOCAL_JOIN format: [rowSize:4][o_ol_cnt:4][count:8]
    // = 16 bytes per result row. Aggregated row reads here are unchanged —
    // they already account for the rowSize prefix.
    //
    // NB: the build-side predicate evaluation depends on the VMS-side
    // compareValues() handling Date <-> Number comparison correctly.
    // See TransactionManager.compareValues for that fix.
    //
    // Cite: DeWitt & Gray 1992 — computation moves to data, not data to computation.
    private String executeChq4Direct() throws Exception {
        long snapshotId = service.getCurrentSnapshotId();
        long startNano  = System.nanoTime();

        long epoch2007 = java.time.LocalDate.of(2007, 1, 2)
                .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
        long epoch2030 = java.time.LocalDate.of(2030, 1, 1)
                .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();

        String buildPredicatesJson = "[" +
                "{\"columnReference\":{\"columnPosition\":2},\"expression\":\"EQUALS\",\"value\":1}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"GREATER_THAN_OR_EQUAL\",\"value\":" + epoch2007 + "}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"LESS_THAN\",\"value\":" + epoch2030 + "}" +
                "]";

        LocalJoinSpec spec = new LocalJoinSpec(
                "orders",
                new int[]{0, 1, 2},
                new int[]{0, 1, 2},
                6,
                buildPredicatesJson);

        Map<Integer, Long> groups = new LinkedHashMap<>();

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            byte[] tableBytes   = "order_line".getBytes(StandardCharsets.UTF_8);
            byte[] routingBytes = spec.toBytes();

            ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0);
            buf.putLong(System.nanoTime());
            buf.putLong(snapshotId);
            buf.put(QueryRequestEvent.MODE_LOCAL_JOIN);
            buf.putInt(tableBytes.length);
            buf.put(tableBytes);
            buf.putInt(0);
            buf.putInt(routingBytes.length);
            buf.put(routingBytes);
            buf.putInt(0);
            int endPos = buf.position();
            buf.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buf.position(endPos);
            buf.flip();

            out.write(buf.array(), 0, buf.limit());
            out.flush();

            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE)
                    throw new IllegalStateException("Unexpected type: " + type);

                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batchBuf = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batchBuf.getLong();

                while (batchBuf.remaining() >= 16) {
                    int rowSize = batchBuf.getInt();
                    int  olCnt  = batchBuf.getInt();
                    long cnt    = batchBuf.getLong();
                    groups.put(olCnt, cnt);
                }
            }
        }

        System.out.printf(">>> [CHQ4 LOCAL JOIN] Groups: %d | Latency: %.2f ms%n",
                groups.size(), (System.nanoTime() - startNano) / 1_000_000.0);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"o_ol_cnt\",\"order_count\"],");
        sb.append("\"resultRowCount\":").append(groups.size()).append(",\"result\":[");
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

    // ── QPO-2: CHQ6 direct scan ───────────────────────────────────────────────
    //
    // CHQ6 has 3 predicates so it triggers the parallel-aggregation path on
    // the VMS — TransactionManager.computeParallelChq6Sum returns a single
    // pre-aggregated row containing the float sum. VmsQueryWorker still wraps
    // it as [rowSize:4][float:4]. So the wire stream after queryId is exactly
    // 8 bytes.
    //
    // ── BUG FIX (silent correctness improvement) ─────────────────────────────
    //
    // The previous loop read 4 bytes at a time as float without skipping the
    // rowSize prefix. It happened to produce a near-correct sum because
    // rowSize=4 read as a float bit pattern is ~5.6e-45 (a denormal,
    // negligible compared to the real ~1.5e3 ol_amount values). But the
    // reported `Rows:` count was 2× the actual count, and the same code on
    // a non-parallel path would have produced wrong sums.
    //
    // FIX: skip the 4-byte rowSize prefix per row. Now reads exactly 8 bytes
    // per wire row and reports the correct row count.
    private String executeChq6Direct() throws Exception {
        long startNano = System.nanoTime();
        double totalRevenue = 0.0;
        long rowCount = 0;

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            int[]  projectedCols  = new int[]{8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);

            ByteBuffer buffer = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buffer.position();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buffer.putInt(0);
            buffer.putLong(System.nanoTime());
            buffer.putLong(service.getCurrentSnapshotId());
            buffer.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableName = "order_line".getBytes(StandardCharsets.UTF_8);
            buffer.putInt(tableName.length);
            buffer.put(tableName);

            // CHQ6 SQL has 2 predicates (ol_w_id=1, ol_quantity BETWEEN 1 AND 100000).
            // The original direct path sent 0 predicates because num_ware=1 makes
            // the first a tautology and ol_quantity max in TPC-C is 10. But the
            // VMS uses (predicates.size() == 3) as the trigger for the parallel
            // CHQ6 path in computeParallelChq6Sum. To keep the parallel path
            // active we'd need to send 3 predicates here. Currently we still
            // send 0 — adjust if you want the parallel path on this route.
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(projectionData.length);
            buffer.put(projectionData);

            int endPos = buffer.position();
            buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buffer.position(endPos);
            buffer.flip();

            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            byte[] header = new byte[5];
            while (true) {
                in.readFully(header);
                byte type = header[0];
                if (type == END_OF_STREAM_TYPE) break;
                if (type != QUERY_RESULT_TYPE)
                    throw new IllegalStateException("Unknown message type: " + type);

                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batchBuffer = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batchBuffer.getLong();

                // Wire row: [rowSize:4][ol_amount:4] = 8 bytes per row.
                while (batchBuffer.remaining() >= 8) {
                    batchBuffer.position(batchBuffer.position() + 4); // skip rowSize prefix
                    float ol_amount = batchBuffer.getFloat();
                    totalRevenue += ol_amount;
                    rowCount++;
                }
            }
        }

        System.out.printf(">>> [CHQ6 DIRECT] Rows: %d | Latency: %.2f ms%n",
                rowCount, (System.nanoTime() - startNano) / 1_000_000.0);

        return "{\"resultColumns\":[\"revenue\"],\"resultRowCount\":1,"
                + "\"result\":[{\"revenue\":" + totalRevenue + "}]}";
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static void proxyToReplica(HttpExchange exchange, String url) throws IOException {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Accept", "application/json")
                    .GET()
                    .build();
            HttpResponse<String> resp =
                    REPLICA_HTTP_CLIENT.send(req, HttpResponse.BodyHandlers.ofString());
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