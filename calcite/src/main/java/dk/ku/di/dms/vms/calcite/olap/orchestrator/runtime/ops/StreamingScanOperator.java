package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.JoinSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ScanSubPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.System.Logger.Level.INFO;

/**
 * A7: StreamingScanOperator now accepts either a ScanSubplan or a JoinSubplan.
 *
 * QPO-3: openScan() now passes scanSubplan.projectionData() to scanWithSchema()
 * so the VMS serializes only the projected columns. null = full scan (backward
 * compatible with the join path which always sends full rows).
 */
public class StreamingScanOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER =
            System.getLogger(StreamingScanOperator.class.getName());

    private static final AtomicLong QUERY_ID_COUNTER = new AtomicLong(0);

    public static long nextQueryId() {
        return QUERY_ID_COUNTER.incrementAndGet();
    }

    private final VmsGatewayClient client;

    private final ScanSubPlan scanSubplan;
    private final JoinSubPlan joinSubplan;

    private final long snapshotId;
    private final long explicitQueryId;

    private Iterator<Object[]> tcpIterator;
    private static final int BATCH_SIZE = 1000;
    private long startTime;
    private long firstByteTime = 0;
    private long endTime;
    private long rowCount = 0;

    /** Plain scan constructor */
    public StreamingScanOperator(VmsGatewayClient client, ScanSubPlan subplan, long snapshotId) {
        this(client, subplan, null, snapshotId, 0L);
    }

    /** Join-receiver constructor — queryId pre-allocated by DistributedExecutor */
    public StreamingScanOperator(VmsGatewayClient client, JoinSubPlan subplan,
                                 long snapshotId, long queryId) {
        this(client, null, subplan, snapshotId, queryId);
    }

    private StreamingScanOperator(VmsGatewayClient client,
                                  ScanSubPlan scanSubplan, JoinSubPlan joinSubplan,
                                  long snapshotId, long queryId) {
        this.client          = client;
        this.scanSubplan     = scanSubplan;
        this.joinSubplan     = joinSubplan;
        this.snapshotId      = snapshotId;
        this.explicitQueryId = queryId;
    }

    @Override
    public void open() {
        this.startTime = System.nanoTime();
        if (scanSubplan != null) {
            openScan();
        } else {
            openJoin();
        }
    }

    private void openScan() {
        String tableName = ((ScanAllOperation) scanSubplan.operation()).table;
        URI    uri       = URI.create(scanSubplan.url());
        String host      = uri.getHost();
        int    port      = uri.getPort();
        long   queryId   = QUERY_ID_COUNTER.incrementAndGet();

        LOGGER.log(INFO, "[StreamingScan] Opening Scan -> " + host + ":" + port
                + " | Table: " + tableName + " | queryId: " + queryId
                + " | snapshotId: " + snapshotId
                + " | projection: " + (scanSubplan.projectionData() != null
                ? scanSubplan.projectionData().length / 4 + " cols" : "ALL"));

        try {
            // QPO-3: pass projectionData — null means full scan (backward compatible)
            this.tcpIterator = client.scanWithSchema(
                    host, port, queryId, snapshotId, (byte) 0,
                    tableName,
                    scanSubplan.columnDescriptors(),
                    scanSubplan.predicates(),
                    new byte[0],                         // routingData: not used for plain scan
                    scanSubplan.projectionData());        // QPO-3: projected columns

            LOGGER.log(INFO, "[StreamingScan] Connection Established (scan).");
        } catch (Exception e) {
            throw new RuntimeException("Failed scan connection to " + host + ":" + port, e);
        }
    }

    private void openJoin() {
        String tableName = ((ScanAllOperation) joinSubplan.operation()).table;
        URI    uri       = URI.create(joinSubplan.url());
        String host      = uri.getHost();
        int    port      = uri.getPort();
        long   queryId   = (explicitQueryId != 0L)
                ? explicitQueryId : QUERY_ID_COUNTER.incrementAndGet();

        LOGGER.log(INFO, "[StreamingScan] Opening Join-Receive -> " + host + ":" + port
                + " | Table: " + tableName + " | queryId: " + queryId
                + " | snapshotId: " + snapshotId);

        try {
            // Join path: no projection — always full rows (both sides need all columns)
            this.tcpIterator = client.scanWithSchema(
                    host, port, queryId, snapshotId, (byte) 2,
                    tableName,
                    joinSubplan.columnDescriptors(),
                    joinSubplan.predicates(),
                    joinSubplan.routingData());   // no projectionData for join
            LOGGER.log(INFO, "[StreamingScan] Connection Established (join).");
        } catch (Exception e) {
            throw new RuntimeException("Failed join connection to " + host + ":" + port, e);
        }
    }

    private String tableName() {
        if (scanSubplan != null) return ((ScanAllOperation) scanSubplan.operation()).table;
        return ((ScanAllOperation) joinSubplan.operation()).table;
    }

    @Override
    public List<Object[]> nextBatch() {
        String tbl = tableName();
        LOGGER.log(INFO, ">>> [StreamingScan] nextBatch() called for table: " + tbl);

        if (this.firstByteTime == 0) {
            this.firstByteTime = System.nanoTime();
            System.out.println("[Metrics] Operator invoke time: "
                    + (firstByteTime - startTime) / 1_000_000.0 + " ms");
        }

        if (tcpIterator == null) return null;

        LOGGER.log(INFO, ">>> [StreamingScan] Blocking on tcpIterator.hasNext() for table: "
                + tbl + "...");
        boolean hasData = tcpIterator.hasNext();
        LOGGER.log(INFO, ">>> [StreamingScan] tcpIterator.hasNext() returned: " + hasData);

        if (!hasData) {
            if (this.endTime == 0 && startTime != 0) {
                this.endTime = System.nanoTime();
                System.out.println("[Metrics] Total Duration: "
                        + (endTime - startTime) / 1_000_000.0 + " ms | Rows: " + rowCount);
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
        LOGGER.log(INFO, "[StreamingScan] Closing operator for table: " + tableName());
    }
}