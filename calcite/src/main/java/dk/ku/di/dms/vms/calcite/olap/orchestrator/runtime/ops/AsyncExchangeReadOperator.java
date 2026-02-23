package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public class AsyncExchangeReadOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(AsyncExchangeReadOperator.class.getName());
    private static final List<Object[]> EOS_MARKER = Collections.emptyList();
    private final VmsHttpClient vmsHttpClient;
    private final VmsSubplan vmsSubplan;
    private final Long snapshot;
    private final BlockingQueue<List<Object[]>> buffer;
    private Thread fetcherThread;
    private boolean isFinished = false;
//    private Set<Object> runtimeFilter;

    public AsyncExchangeReadOperator(VmsHttpClient vmsHttpClient, VmsSubplan vmsSubplan, Long snapshot) {
        this.vmsHttpClient = vmsHttpClient;
        this.vmsSubplan = vmsSubplan;
        this.snapshot = snapshot;
        this.buffer = new ArrayBlockingQueue<>(20);
    }

//    public void setDynamicFilter(Set<Object> keys) {
//        this.runtimeFilter = keys;
//    }

    @Override
    public void open() {
        this.fetcherThread = new Thread(this::fetchLoop, "fetcher-" + vmsSubplan.exchangeId);
        this.fetcherThread.start();
        LOGGER.log(INFO, "[Exchange:" + vmsSubplan.exchangeId + "] Background Fetcher Started.");
    }

    private void fetchLoop() {
        try {
            LOGGER.log(INFO, "Exchange:" + vmsSubplan.exchangeId + " Sending HTTP Request to " + vmsSubplan.vmsName);

            // Execute HTTP Request
            PushdownResponse response = vmsHttpClient.executeScanAll(vmsSubplan, snapshot);

            if (response.rows != null) {
                // Stream rows into buffer
                for (List<Object> row : response.rows) {
                    buffer.put(Collections.singletonList(row.toArray()));
                }
            }

            buffer.put(EOS_MARKER);
            LOGGER.log(INFO, "[Exchange:" + vmsSubplan.exchangeId + "] Fetching Complete. EOS buffered.");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.log(ERROR, "Error in fetcher", e);
            try { buffer.put(EOS_MARKER); } catch (InterruptedException ignored) {}
        }
    }

    //dont just wait
    @Override
    public List<Object[]> nextBatch() {
        if (isFinished) {
            return null;
        }

        try {
            List<Object[]> batch = buffer.take();

            if (batch == EOS_MARKER) {
                isFinished = true;
                return null;
            }
            return batch;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    @Override
    public void close() {
        if (fetcherThread != null) {
            fetcherThread.interrupt();
        }
    }
}