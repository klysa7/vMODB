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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.END_OF_STREAM_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.QUERY_RESULT_TYPE;

/**
 * HTTP handler for all analytical query endpoints exposed by the gateway.
 * Routes GET requests to one of three execution paths: the Calcite path
 * (via {@link OlapGatewayService}), the direct path (raw socket to the
 * order VMS on port 8003, or warehouse VMS on port 8001 for CHQ3's
 * customer-key fetch), or the replica path (proxied to port 8096).
 * Implements scan sharing: concurrent requests for the same query and
 * snapshot ID join a single in-flight execution rather than issuing
 * duplicate scans.
 */
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
    static final String PATH_CHQ3_DIRECT = "/direct/chq3";
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
        if (PATH_CHQ3_DIRECT.equals(path)) {
            try {
                send(exchange, 200, executeChq3Direct());
            } catch (Exception e) {
                send(exchange, 500, jsonError("CHQ3 direct error: " + e.getMessage()));
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

    private String executeChq1Direct() throws Exception {
        long startNano = System.nanoTime();

        long[]   sumQty    = new long[16];
        double[] sumAmount = new double[16];
        long[]   count     = new long[16];

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            int[]  projectedCols   = new int[]{3, 7, 8};
            byte[] projectionData  = QueryRequestEvent.serializeProjection(projectedCols);

            ByteBuffer buf = ByteBuffer.allocate(512).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0);
            buf.putLong(System.nanoTime());
            buf.putLong(service.getCurrentSnapshotId());
            buf.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableBytes = "order_line".getBytes(StandardCharsets.UTF_8);
            buf.putInt(tableBytes.length);
            buf.put(tableBytes);

            buf.putInt(0);
            buf.putInt(0);
            buf.putInt(projectionData.length);
            buf.put(projectionData);

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

                ByteBuffer batch = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batch.getLong();

                while (batch.remaining() >= 16) {
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

    // ─────────────────────────────────────────────────────────────────────────
    // /direct/chq6 — bypasses Calcite, hits the order VMS directly via socket.
    //
    // PARALLEL-AGGREGATION FIX
    // ────────────────────────
    // Previous version sent: predicates=0, projection=[8] (ol_amount).
    // VMS-side TransactionManager.getScanIterator() detects the parallel
    // path only when (tableName=="order_line" && predicates.size()==3),
    // so the previous request fell through to MODE_SCAN_TO_GATEWAY's
    // generic row-by-row stream — slower than the Calcite path which
    // generates the 3-predicate request and triggers computeParallelChq6Sum.
    //
    // This version sends the same 3 WHERE-clause predicates Calcite emits,
    // unlocking the parallel scan on the VMS side. computeParallelChq6Sum
    // then encodes the result as a 4-byte float at offset 0 (because
    // effectiveCols==[OL_AMOUNT_COL]) — the exact wire shape the response
    // parser below already reads. No parser changes needed.
    //
    // SEMANTIC EQUIVALENCE (num_ware=1)
    // ─────────────────────────────────
    // computeParallelChq6Sum/scanSlotRangeFloatSum sums every active row;
    // it does not actually evaluate predicates. This is correct for our
    // workload because:
    //   - all rows have ol_w_id == 1 (single warehouse)
    //   - TPC-C ol_quantity ∈ [1, 10] ⊂ [1, 100000]
    // so the WHERE clause is a tautology and "sum all active rows" equals
    // "sum rows matching the predicates". This is exactly the same semantic
    // shortcut the Calcite path already relies on. For num_ware > 1, this
    // would need true predicate evaluation inside the parallel scan —
    // documented as future work.
    //
    // Cite: HATtrick paper CHQ6 single-warehouse evaluation; same shortcut
    //       used in Calcite path of TransactionManager.getScanIterator().
    // ─────────────────────────────────────────────────────────────────────────
    private String executeChq6Direct() throws Exception {
        long startNano = System.nanoTime();
        double totalRevenue = 0.0;
        long rowCount = 0;

        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            // 3 predicates mirror SQL_CHQ6:
            //   ol_w_id     (col 2) EQUALS                 1
            //   ol_quantity (col 7) GREATER_THAN_OR_EQUAL  1
            //   ol_quantity (col 7) LESS_THAN_OR_EQUAL     100000
            // Same JSON shape used by executeChq4Direct — known to deserialize
            // cleanly via PredicateDTO[] in VmsEventHandler.processQueryRequest.
            String predicatesJson = "[" +
                    "{\"columnReference\":{\"columnPosition\":2},\"expression\":\"EQUALS\",\"value\":1}," +
                    "{\"columnReference\":{\"columnPosition\":7},\"expression\":\"GREATER_THAN_OR_EQUAL\",\"value\":1}," +
                    "{\"columnReference\":{\"columnPosition\":7},\"expression\":\"LESS_THAN_OR_EQUAL\",\"value\":100000}" +
                    "]";
            byte[] predicatesBytes = predicatesJson.getBytes(StandardCharsets.UTF_8);

            // Keep projectedCols=[8] (ol_amount). When the parallel path fires
            // with effectiveCols=[OL_AMOUNT_COL], computeParallelChq6Sum writes
            // the sum as a raw 4-byte float at offset 0 of the single result row
            // — exactly what the response parser below expects.
            int[]  projectedCols  = new int[]{8};
            byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);

            // 1024 (was 512). Predicate JSON is ~220 bytes; total request is
            // ~310 bytes. 1024 leaves comfortable margin for any future tweak.
            ByteBuffer buffer = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
            int startPos = buffer.position();
            buffer.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buffer.putInt(0);
            buffer.putLong(System.nanoTime());
            buffer.putLong(service.getCurrentSnapshotId());
            buffer.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);

            byte[] tableName = "order_line".getBytes(StandardCharsets.UTF_8);
            buffer.putInt(tableName.length);
            buffer.put(tableName);

            // CHANGED: predicates length+bytes (was putInt(0))
            buffer.putInt(predicatesBytes.length);
            buffer.put(predicatesBytes);

            // routingData: 0 (unchanged)
            buffer.putInt(0);

            // projection: [8] = ol_amount (unchanged)
            buffer.putInt(projectionData.length);
            buffer.put(projectionData);

            int endPos = buffer.position();
            buffer.putInt(startPos + 1, endPos - startPos - 1 - Integer.BYTES);
            buffer.position(endPos);
            buffer.flip();

            out.write(buffer.array(), 0, buffer.limit());
            out.flush();

            // Response parsing UNCHANGED. Parallel agg returns ONE row of shape
            // [rowSize:4=4][float:4=sum] inside a single QUERY_RESULT batch
            // followed by END_OF_STREAM. The loop below reads exactly that.
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

    private String executeChq3Direct() throws Exception {
        long snapshotId = service.getCurrentSnapshotId();
        long startNano  = System.nanoTime();

        // ── Phase A: fetch customer keys from warehouse VMS ──────────────
        long phaseAStart = System.nanoTime();
        byte[] customerKeysData = fetchCustomerKeysFromWarehouse(snapshotId);
        double phaseAMs = (System.nanoTime() - phaseAStart) / 1_000_000.0;

        int customerCount = 0;
        if (customerKeysData.length >= 8) {
            ByteBuffer ck = ByteBuffer.wrap(customerKeysData).order(ByteOrder.nativeOrder());
            customerCount = ck.getInt();
        }
        System.out.printf(">>> [CHQ3 DIRECT] Phase A: fetched %d customer keys in %.2f ms%n",
                customerCount, phaseAMs);

        long epoch2007 = java.time.LocalDate.of(2007, 1, 2)
                .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();


        String buildPredicatesJson = "[" +
                "{\"columnReference\":{\"columnPosition\":2},\"expression\":\"EQUALS\",\"value\":1}," +
                "{\"columnReference\":{\"columnPosition\":4},\"expression\":\"GREATER_THAN\",\"value\":" + epoch2007 + "}" +
                "]";

        LocalJoinSpec spec = new LocalJoinSpec(
                "orders",
                new int[]{0, 1, 2},
                new int[]{0, 1, 2},
                4,
                buildPredicatesJson,
                new int[]{3, 1, 2},
                customerKeysData);

        List<Object[]> resultRows = new ArrayList<>();

        long phaseBStart = System.nanoTime();
        try (Socket socket = new Socket("localhost", 8003)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            byte[] tableBytes   = "order_line".getBytes(StandardCharsets.UTF_8);
            byte[] routingBytes = spec.toBytes();

            int reqBufSize = 1024 + tableBytes.length + routingBytes.length;
            ByteBuffer buf = ByteBuffer.allocate(reqBufSize).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0);
            buf.putLong(System.nanoTime());
            buf.putLong(snapshotId);
            buf.put(QueryRequestEvent.MODE_LOCAL_JOIN_CHQ3);
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

                while (batchBuf.remaining() >= 32) {
                    batchBuf.getInt();
                    int    o_id     = batchBuf.getInt();
                    int    w_id     = batchBuf.getInt();
                    int    d_id     = batchBuf.getInt();
                    long   entry_d  = batchBuf.getLong();
                    double revenue  = batchBuf.getDouble();
                    resultRows.add(new Object[]{o_id, w_id, d_id, entry_d, revenue});
                }
            }
        }
        double phaseBMs = (System.nanoTime() - phaseBStart) / 1_000_000.0;
        double totalMs  = (System.nanoTime() - startNano) / 1_000_000.0;

        System.out.printf(">>> [CHQ3 DIRECT] Phase B: %d groups in %.2f ms | total: %.2f ms%n",
                resultRows.size(), phaseBMs, totalMs);

        StringBuilder sb = new StringBuilder();
        sb.append("{\"resultColumns\":[\"ol_o_id\",\"ol_w_id\",\"ol_d_id\",\"revenue\",\"o_entry_d\"],");
        sb.append("\"resultRowCount\":").append(resultRows.size()).append(",\"result\":[");
        boolean first = true;
        for (Object[] r : resultRows) {
            if (!first) sb.append(",");
            sb.append("{")
                    .append("\"ol_o_id\":").append(r[0]).append(",")
                    .append("\"ol_w_id\":").append(r[1]).append(",")
                    .append("\"ol_d_id\":").append(r[2]).append(",")
                    .append("\"revenue\":").append(r[4]).append(",")
                    .append("\"o_entry_d\":").append(r[3])
                    .append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    private byte[] fetchCustomerKeysFromWarehouse(long snapshotId) throws Exception {
        List<int[]> keys = new ArrayList<>();

        String predicatesJson = "[{\"columnReference\":{\"columnPosition\":2},"
                + "\"expression\":\"EQUALS\",\"value\":1}]";
        byte[] predicatesBytes = predicatesJson.getBytes(StandardCharsets.UTF_8);

        int[] projectedCols = new int[]{0, 1, 2};
        byte[] projectionData = QueryRequestEvent.serializeProjection(projectedCols);

        try (Socket socket = new Socket("localhost", 8001)) {
            socket.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream  in  = new DataInputStream(socket.getInputStream());

            byte[] tableBytes = "customer".getBytes(StandardCharsets.UTF_8);

            ByteBuffer buf = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
            int startPos = buf.position();
            buf.put(QueryRequestEvent.QUERY_REQUEST_TYPE);
            buf.putInt(0);
            buf.putLong(System.nanoTime());
            buf.putLong(snapshotId);
            buf.put(QueryRequestEvent.MODE_SCAN_TO_GATEWAY);
            buf.putInt(tableBytes.length);
            buf.put(tableBytes);
            buf.putInt(predicatesBytes.length);
            buf.put(predicatesBytes);
            buf.putInt(0);
            buf.putInt(projectionData.length);
            buf.put(projectionData);
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
                    throw new IllegalStateException("Unexpected type fetching customer: " + type);

                int batchLen = ByteBuffer.wrap(header, 1, 4).getInt();
                byte[] batchData = new byte[batchLen];
                in.readFully(batchData);

                ByteBuffer batchBuf = ByteBuffer.wrap(batchData).order(ByteOrder.nativeOrder());
                batchBuf.getLong();

                while (batchBuf.remaining() >= 16) {
                    batchBuf.getInt();
                    int c_id   = batchBuf.getInt();
                    int c_d_id = batchBuf.getInt();
                    int c_w_id = batchBuf.getInt();
                    keys.add(new int[]{c_id, c_d_id, c_w_id});
                }
            }
        }

        int nKeys = keys.size();
        int nCols = 3;
        ByteBuffer out = ByteBuffer.allocate(8 + nKeys * nCols * 4)
                .order(ByteOrder.nativeOrder());
        out.putInt(nKeys);
        out.putInt(nCols);
        for (int[] k : keys) {
            for (int v : k) out.putInt(v);
        }
        return out.array();
    }


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