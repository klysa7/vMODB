package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public class StreamingScanOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(StreamingScanOperator.class.getName());

    private final VmsGatewayClient client;
    private final VmsSubplan subplan;
    private final long snapshotId;

    private Iterator<Object[]> tcpIterator;
    private static final int BATCH_SIZE = 1000;
    private long startTime;
    private long firstByteTime = 0;
    private long endTime;
    private long rowCount = 0;

    public StreamingScanOperator(VmsGatewayClient client, VmsSubplan subplan, long snapshotId) {
        this.client = client;
        this.subplan = subplan;
        this.snapshotId = snapshotId;
    }



    @Override
    public void open() {
        this.startTime = System.nanoTime();

        if (!(subplan.operation instanceof ScanAllOperation)) {
            throw new IllegalArgumentException("StreamingScanOperator only supports ScanAllOperation");
        }
        String tableName = ((ScanAllOperation) subplan.operation).table;

        String host = "localhost";
        int tcpPort = resolveTpccPort(tableName);

        LOGGER.log(INFO, "[StreamingScan] Opening Connection -> " + host + ":" + tcpPort + " | Table: " + tableName + " | Mode: " + subplan.mode);

        List<Class<?>> types = new ArrayList<>();
        for (String col : subplan.columnsInOrder) {
            types.add(String.class);
        }

        try {
            // FIX: We now pass subplan.mode and subplan.routingData to match the new signature!
            this.tcpIterator = client.scan(host, tcpPort, snapshotId, snapshotId, subplan.mode, tableName, types, subplan.predicates, subplan.routingData);

            LOGGER.log(INFO, "[StreamingScan] Connection Established. Iterator ready.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to open streaming connection to " + host + ":" + tcpPort, e);
        }
    }

    /**
     * Maps table names to the 800x ports currently used by the VMS listeners.
     */
    private int resolveTpccPort(String tableName) {
        tableName = tableName.toLowerCase();

        if (tableName.contains("warehouse") ||
                tableName.contains("district") ||
                tableName.contains("customer") ||
                tableName.contains("history")) {
            return 8001;
        }

        if (tableName.contains("item") ||
                tableName.contains("stock")) {
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
        if (subplan != null && subplan.operation instanceof ScanAllOperation) {
            String tableName = ((ScanAllOperation) subplan.operation).table;
            LOGGER.log(INFO, "[StreamingScan] Closing operator for table: " + tableName);
        }
    }
}