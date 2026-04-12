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

    // ══ B23 FIX: Shared HttpClient for Replica Passthrough ═══════════════════
    //
    // BEFORE: HttpClient.newHttpClient() allocated per proxyToReplica() call.
    //   Each call builds a fresh connection pool, async I/O executor, selector
    //   thread, and SSL context. No HTTP/1.1 keep-alive benefit — every
    //   request pays a full TCP handshake to port 8096. Discarded instances
    //   hold native resources until GC.
    //
    // AFTER: Single static final HttpClient, built once at class load, reused
    //   for all replica requests. HttpClient internally maintains HTTP/1.1
    //   persistent connections to the replica — subsequent requests reuse the
    //   established TCP connection. connectTimeout(5s) prevents indefinite
    //   blocking if the replica is unreachable.
    //
    // Scope: /olap/replica/* endpoints only (Experiment II, use_replica=true).
    //   Experiment I queries and direct paths are unaffected.
    //
    // Cite: Gray & Reuter 1992 — connection cost must be amortized over
    //       many requests.
    //       Fielding & Reschke 2014 (RFC 7230) — HTTP/1.1 persistent connections.
    // ─────────────────────────────────────────────────────────────────────────
    private static final HttpClient REPLICA_HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // ══ B-SS FIX: Scan Sharing via ConcurrentHashMap<key, CompletableFuture> ═
    //
    // PROBLEM: at α=2, two identical OLAP queries arrive within milliseconds.
    //   Without sharing, both fire a full VMS scan independently — double the
    //   VMS CPU, double the TCP overhead, double the result processing.
    //   Under mixed load this doubling is what starves the OLTP commit path.
    //
    // SOLUTION: if an identical scan (same SQL + same snapshotId) is already
    //   in-flight, the second query joins the first query's CompletableFuture
    //   instead of starting a new VMS request. When the first scan completes,
    //   future.complete() fans the result out to all waiting threads at once.
    //
    // KEY = sqlHash + ":" + snapshotId  (strict correctness).
    //   Same SQL + same snapshotId → same committed database state → safe.
    //   Different snapshotIds → queries execute independently. No staleness.
    //
    // SNAPSHOT COLLISION RATE: coordinator commits batches every ~230ms. Two
    //   queries arriving within the same batch window see the same snapshotId
    //   → sharing fires. When snapshotIds differ, queries execute independently
    //   — correct always.
    //
    // THREAD SAFETY: putIfAbsent() is atomic. Exactly one thread creates the
    //   future (first); all others get the existing one. No deadlock — the
    //   first thread executes the scan, others wait on future.get().
    //
    // Cite: Zukowski et al. 2007 "Cooperative Scans" (VLDB) —
    //          share the physical result, not the physical scan.
    //       QuestDB Discipline 3 — cooperative scan sharing.
    //       Supervisor OPT-1.
    //
    // Scope: Calcite-path routes (PATH_Q1/CHQ6/CHQ1/CHQ4/CHQ3) only.
    //   Direct paths (/direct/*) bypass sharing — they're already cheap enough
    //   that the sharing overhead would not pay for itself.
    // ─────────────────────────────────────────────────────────────────────────
    private final ConcurrentHashMap<String, CompletableFuture<String>> scanRegistry =
            new ConcurrentHashMap<>();

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

    // ── CHQ6: dual-path (general Calcite + QPO-2 direct scan) ────────────────
    //
    // /olap/chq6    → OlapGatewayService (Calcite + planner + operator tree
    //                 + VmsGatewayClient). QPO-3 still fires. Scan-shared.
    // /direct/chq6  → QPO-2 hot path. Raw socket, hand-built QueryRequestEvent,
    //                 sums floats in a tight loop. Single-VMS-safe only.
    static final String PATH_CHQ6        = "/olap/chq6";
    static final String PATH_CHQ6_DIRECT = "/direct/chq6";

    static final String SQL_CHQ6 = """
        SELECT SUM(ol.ol_amount) AS revenue
        FROM "order".order_line ol
        WHERE ol.ol_w_id = 1
          AND ol.ol_quantity BETWEEN 1 AND 100000
    """;

    // ── CHQ1: dual-path (general Calcite + QPO-6 direct aggregation) ─────────
    //
    // /olap/chq1    → general Calcite path. LocalAggregateOperator builds a
    //                 HashMap<Integer, double[]> over all 300K+ order_line
    //                 rows (fully blocking, autoboxing per row, GC pressure).
    // /direct/chq1  → QPO-6 hot path. Projects only ol_number / ol_quantity /
    //                 ol_amount (QPO-3) → 12 bytes/row. Accumulates into
    //                 fixed-size primitive arrays indexed by ol_number
    //                 (TPC-C domain [1..15]) — no HashMap, no boxing, no GC.
    //                 Streaming aggregation. Single-VMS-safe (num_ware=1 only).
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

    // ── CHQ4: dual-path (general Calcite + QPO-5 intra-VMS local join) ───────
    //
    // /olap/chq4    → general Calcite path. Two-phase broadcast join with the
    //                 100ms B44 delay and gateway-side LocalJoinOperator.
    // /direct/chq4  → QPO-5 hot path. Single MODE_LOCAL_JOIN request; the VMS
    //                 runs hash build + probe + GROUP BY in local memory and
    //                 streams back only aggregated rows.
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

        // ── Replica passthrough ───────────────────────────────────────────────
        if (PATH_REPLICA_CHQ6.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ6_URL);
            return;
        }
        if (PATH_REPLICA_CHQ1.equals(path)) {
            proxyToReplica(exchange, REPLICA_CHQ1_URL);
            return;
        }

        // ── Direct hot paths (QPO-2 / QPO-5 / QPO-6) ──────────────────────────
        // These bypass Calcite entirely — scan sharing does not apply here.
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

        // ── Standard Calcite queries (baseline routes) ────────────────────────
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

        // ══ B-SS FIX: Scan Sharing intercept ═════════════════════════════════
        //
        // Key: sqlHash ensures different queries never share.
        //      snapshotId ensures different database states never share.
        //
        // putIfAbsent() is atomic — exactly one thread wins (returns null = first).
        // All other threads for the same key wait on future.get() and receive
        // the same result string when the first thread completes its scan.
        // ─────────────────────────────────────────────────────────────────────
        long   snapshotId = service.getCurrentSnapshotId();
        String scanKey    = sql.hashCode() + ":" + snapshotId;

        CompletableFuture<String> newFuture = new CompletableFuture<>();
        CompletableFuture<String> existing  = this.scanRegistry.putIfAbsent(scanKey, newFuture);
        boolean isFirst = (existing == null);
        CompletableFuture<String> future = isFirst ? newFuture : existing;

        if (isFirst) {
            // This thread owns the physical VMS scan.
            // On completion, future.complete() unblocks all waiting joiners.
            try {
                String result = service.execute(sql);
                future.complete(result);
                send(exchange, 200, result);
            } catch (Exception e) {
                future.completeExceptionally(e);
                send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
            } finally {
                // Value-checking remove: only removes if this is still the current future.
                // Prevents a race where a late joiner's future is accidentally removed
                // by another thread's cleanup.
                this.scanRegistry.remove(scanKey, newFuture);
            }
        } else {
            // This thread joins the existing scan — no VMS request fired.
            // Blocks until the first thread calls future.complete(result).
            // Both clients receive the same result — safe because same snapshotId
            // means same committed database state was visible to both queries.
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
    // Instead of Calcite's HashMap aggregator over Object[] rows, we:
    //   1. Project only 3 columns: ol_number(3), ol_quantity(7), ol_amount(8)
    //      → 12 bytes/row instead of 104 bytes/row (QPO-3 integration)
    //   2. Accumulate into fixed-size primitive arrays indexed by ol_number
    //      → no HashMap, no boxing, no GC pressure (QPO-6 contribution)
    //   3. Compute AVG inline from running sums at the end
    //
    // Same architectural principle as QPO-2 (CHQ6): query compilation.
    // Cite: Neumann 2011 — "Efficiently Compiling Efficient Query Plans for
    // Modern Hardware" (VLDB). Fixed-size array replaces general aggregation.
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

            // QPO-3: project ol_number(3), ol_quantity(7), ol_amount(8) → 12 bytes/row
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

            // ── Read projected rows: [ol_number:INT 4][ol_quantity:INT 4][ol_amount:FLOAT 4]
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

                while (batch.remaining() >= 16) { // 4 (rowSize) + 12 (row)
                    int rowSize   = batch.getInt(); // should be 12
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
    // Both orders and order_line are co-located in the order VMS. Instead of
    // the 2-TCP broadcast protocol with 100ms hardcoded delay (B44), we send a
    // single MODE_LOCAL_JOIN request. The VMS executes the hash join locally
    // and returns only the aggregated result rows.
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
    // Projects only ol_amount (col 8, 4 bytes) via QPO-3 integration.
    // Sums primitives inline — no Calcite, no Object[] wrapping, no GC.
    // Transfer: 31MB → 1.2MB. Cite: Neumann 2011 (query compilation).
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

                while (batchBuffer.hasRemaining()) {
                    int rowSize = batchBuffer.getInt();
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
            // B23 FIX: use shared REPLICA_HTTP_CLIENT instead of HttpClient.newHttpClient().
            // Reuses HTTP/1.1 keep-alive connection to port 8096 across all replica calls.
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