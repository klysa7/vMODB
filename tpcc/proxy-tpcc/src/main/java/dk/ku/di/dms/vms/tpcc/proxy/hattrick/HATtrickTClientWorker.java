package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HATtrick T-client — coordinator-aware backpressure, no submission throttle.
 *
 * Throttling is now performed at the coordinator level (in
 * TransactionWorker.run() via the batch_sleep_ms property), not on the
 * T-client side. This implements the professor's "for every batch sleep
 * a little bit, allow batch to complete" idea more literally — the
 * coordinator pauses between batches so VMSes get clear quiet periods
 * for OLAP scans. The T-client just submits as fast as backpressure allows.
 *
 * BACKPRESSURE BUDGET (maxInFlight)
 *   The runner passes a tau-scaled budget — MAX_IN_FLIGHT_PER_CLIENT × τ.
 *   When in-flight > maxInFlight, Thread.yield() gives the coordinator
 *   thread CPU time to drain a batch.
 */
public final class HATtrickTClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickTClientWorker.class.getName());

    /** Default budget if the legacy 4-arg constructor is used. */
    private static final int DEFAULT_MAX_IN_FLIGHT = 5_000;

    private final int           clientId;
    private final Coordinator   coordinator;
    private final AtomicBoolean running;
    private final int           numWarehouses;
    private final int           maxInFlight;

    private long submitted = 0L;
    private long lastPrintAt = 0;

    public HATtrickTClientWorker(int clientId,
                                 Coordinator coordinator,
                                 AtomicBoolean running,
                                 int numWarehouses,
                                 int maxInFlight) {
        this.clientId      = clientId;
        this.coordinator   = coordinator;
        this.running       = running;
        this.numWarehouses = numWarehouses;
        this.maxInFlight   = maxInFlight;
    }

    /** Backwards-compatible 4-arg constructor. */
    public HATtrickTClientWorker(int clientId,
                                 Coordinator coordinator,
                                 AtomicBoolean running,
                                 int numWarehouses) {
        this(clientId, coordinator, running, numWarehouses, DEFAULT_MAX_IN_FLIGHT);
    }

    @Override
    public void run() {
        LOG.log(System.Logger.Level.INFO,
                "T-client {0} started (maxInFlight={1})",
                clientId, maxInFlight);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                long inFlight = coordinator.getTotalInflightLoad();
                if (inFlight > maxInFlight) {
                    Thread.yield();
                    continue;
                }

                TransactionInput txInput;
                if (DataGenUtils.randomNumber(1, 2) == 1) {
                    NewOrderWareIn event = generateNewOrder();
                    txInput = new TransactionInput(
                            "new_order",
                            new TransactionInput.Event("new-order-ware-in", event.toString()));
                } else {
                    PaymentIn event = generatePayment();
                    txInput = new TransactionInput(
                            "payment",
                            new TransactionInput.Event("payment-in", event.toString()));
                }

                coordinator.queueTransactionInput(txInput);
                submitted++;

                long now = System.currentTimeMillis();
                if (now - lastPrintAt >= 5000) {
                    System.out.printf("[T-client %d] Submitted %,d transactions total%n",
                            clientId, submitted);
                    lastPrintAt = now;
                }

            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) break;
                LOG.log(System.Logger.Level.WARNING,
                        "T-client {0} error: {1}", clientId, e.getMessage());
            }
        }

        LOG.log(System.Logger.Level.INFO,
                "T-client {0} stopped. Submitted {1} txns",
                clientId, submitted);
    }

    public long getSubmittedCount() {
        return submitted;
    }

    private NewOrderWareIn generateNewOrder() {
        int w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        int ol_cnt = DataGenUtils.randomNumber(
                TPCcConstants.MIN_NUM_ITEMS_PER_ORDER, TPCcConstants.MAX_NUM_ITEMS_PER_ORDER);

        int[] itemIds  = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty      = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            itemIds[i]  = DataGenUtils.nuRand(8191, 7911, 1, TPCcConstants.NUM_ITEMS);
            qty[i]      = DataGenUtils.randomNumber(1, 10);
            supWares[i] = w_id;
        }
        return new NewOrderWareIn(w_id, d_id, c_id, itemIds, supWares, qty, true);
    }

    private PaymentIn generatePayment() {
        int   w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int   d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int   c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        float amount = DataGenUtils.randomNumber(100, 500000) / 100.0f;
        return new PaymentIn(w_id, d_id, c_id, w_id, d_id, amount, "", false);
    }
}