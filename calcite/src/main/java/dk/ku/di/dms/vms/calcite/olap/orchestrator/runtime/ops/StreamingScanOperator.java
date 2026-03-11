package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.client.ColumnDescriptor;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.INFO;

public class StreamingScanOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(StreamingScanOperator.class.getName());

    // -------------------------------------------------------------------------
    // B3 FIX: queryId must be distinct from snapshotId, AND must be the same
    // value on both the probe scan and the corresponding broadcast trigger.
    //
    // The counter is allocated ONCE per join pair by the caller
    // (DistributedExecutor.buildOperatorTree) via nextQueryId(), then passed
    // explicitly to both this constructor and triggerBroadcast().
    // For plain (non-join) scans the no-queryId constructor auto-increments.
    // -------------------------------------------------------------------------
    private static final AtomicLong QUERY_ID_COUNTER = new AtomicLong(0);

    /** Allocate a globally-unique queryId. Called by DistributedExecutor before
     *  constructing both the receiver StreamingScanOperator and the broadcast
     *  trigger, so they share the same routing key. */
    public static long nextQueryId() {
        return QUERY_ID_COUNTER.incrementAndGet();
    }

    private final VmsGatewayClient client;
    private final VmsSubplan subplan;
    private final long snapshotId;
    private final long explicitQueryId; // 0 means "auto-increment in open()"

    private Iterator<Object[]> tcpIterator;
    private static final int BATCH_SIZE = 1000;
    private long startTime;
    private long firstByteTime = 0;
    private long endTime;
    private long rowCount = 0;

    /** Plain scan — queryId is auto-generated in open(). */
    public StreamingScanOperator(VmsGatewayClient client, VmsSubplan subplan, long snapshotId) {
        this(client, subplan, snapshotId, 0L);
    }

    /** Join-receiver scan — queryId pre-allocated by DistributedExecutor so it
     *  matches the queryId passed to triggerBroadcast on the build side. */
    public StreamingScanOperator(VmsGatewayClient client, VmsSubplan subplan, long snapshotId, long queryId) {
        this.client = client;
        this.subplan = subplan;
        this.snapshotId = snapshotId;
        this.explicitQueryId = queryId;
    }

    @Override
    public void open() {
        this.startTime = System.nanoTime();

        if (!(subplan.operation instanceof ScanAllOperation)) {
            throw new IllegalArgumentException("StreamingScanOperator only supports ScanAllOperation");
        }
        String tableName = ((ScanAllOperation) subplan.operation).table;
        String host      = "localhost";
        int    tcpPort   = resolveTpccPort(tableName);

        // B3 FIX: use the pre-allocated queryId if one was passed explicitly
        // (join-receiver case), otherwise auto-increment for plain scans.
        long queryId = (explicitQueryId != 0L) ? explicitQueryId : QUERY_ID_COUNTER.incrementAndGet();

        LOGGER.log(INFO, "[StreamingScan] Opening Connection -> " + host + ":" + tcpPort
                + " | Table: " + tableName + " | Mode: " + subplan.mode
                + " | queryId: " + queryId + " | snapshotId: " + snapshotId);

        try {
            if (subplan.columnDescriptors != null) {
                this.tcpIterator = client.scanWithSchema(
                        host, tcpPort, queryId, snapshotId, subplan.mode,
                        tableName, subplan.columnDescriptors,
                        subplan.predicates, subplan.routingData);
            } else {
                this.tcpIterator = client.scan(
                        host, tcpPort, queryId, snapshotId, subplan.mode,
                        tableName, List.<Class<?>>of(),
                        subplan.predicates, subplan.routingData);
            }
            LOGGER.log(INFO, "[StreamingScan] Connection Established. Iterator ready.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to open streaming connection to "
                    + host + ":" + tcpPort, e);
        }
    }

    private int resolveTpccPort(String tableName) {
        tableName = tableName.toLowerCase();
        if (tableName.contains("warehouse") ||
                tableName.contains("district") ||
                tableName.contains("customer") ||
                tableName.contains("history")) {
            return 8001;
        }
        if (tableName.contains("item") || tableName.contains("stock")) {
            return 8002;
        }
        if (tableName.contains("order") ||
                tableName.contains("new_orders") ||
                tableName.contains("order_line")) {
            return 8003;
        }
        try {
            if (subplan.url != null) {
                return URI.create(subplan.url).getPort();
            }
        } catch (Exception ignored) {}
        return 8001;
    }

    @Override
    public List<Object[]> nextBatch() {
        String tableName = ((ScanAllOperation) subplan.operation).table;
        LOGGER.log(INFO, ">>> [StreamingScan] nextBatch() called for table: " + tableName);

        if (this.firstByteTime == 0) {
            this.firstByteTime = System.nanoTime();
            double ttfbMs = (firstByteTime - startTime) / 1_000_000.0;
            System.out.println("[Metrics] Operator invoke time (NOT real TTFB): " + ttfbMs + " ms");
        }

        if (tcpIterator == null) {
            LOGGER.log(INFO, ">>> [StreamingScan] tcpIterator is NULL! Returning null.");
            return null;
        }

        LOGGER.log(INFO, ">>> [StreamingScan] Blocking on tcpIterator.hasNext() for table: " + tableName + "...");
        boolean hasData = tcpIterator.hasNext();
        LOGGER.log(INFO, ">>> [StreamingScan] tcpIterator.hasNext() returned: " + hasData);

        if (!hasData) {
            if (this.endTime == 0 && startTime != 0) {
                this.endTime = System.nanoTime();
                double totalMs = (endTime - startTime) / 1_000_000.0;
                System.out.println("[Metrics] Total Duration: " + totalMs + " ms | Rows: " + rowCount);
            }
            LOGGER.log(INFO, ">>> [StreamingScan] No more data. Returning null.");
            return null;
        }

        List<Object[]> batch = new ArrayList<>(BATCH_SIZE);
        int count = 0;
        while (tcpIterator.hasNext() && count < BATCH_SIZE) {
            batch.add(tcpIterator.next());
            count++;
            rowCount++;
        }

        LOGGER.log(INFO, ">>> [StreamingScan] Returning batch of size: " + count);
        return batch;
    }

    @Override
    public void close() {
        if (subplan != null && subplan.operation instanceof ScanAllOperation op) {
            LOGGER.log(INFO, "[StreamingScan] Closing operator for table: " + op.table);
        }
    }
}