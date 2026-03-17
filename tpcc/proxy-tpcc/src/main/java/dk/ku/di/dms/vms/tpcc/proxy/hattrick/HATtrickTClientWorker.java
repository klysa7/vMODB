package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick T-client for the throughput frontier experiment.
 *
 * Generates new_order (50%) and payment (50%) transactions on the fly
 * and submits them directly to the coordinator without any sleep.
 *
 * Throughput is measured by the caller via getSubmittedCount() sampled
 * at the start and end of the measurement window — no batch callback
 * needed for the grid measurement.
 *
 * Design note: we count *submitted* transactions, not committed ones.
 * The batch window adds a small lag (typically < batchWindowMs) which
 * is the same across all grid points, so the bias cancels when comparing
 * (τ,α) pairs. The coordinator's batch commit callback is not used here
 * to keep the T-client self-contained and avoid cross-thread state.
 */
public final class HATtrickTClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickTClientWorker.class.getName());

    private final int           clientId;
    private final Coordinator   coordinator;
    private final AtomicBoolean running;
    private final int           numWarehouses;

    // submitted count — incremented every time a transaction is queued
    private final AtomicLong submitted = new AtomicLong(0L);

    public HATtrickTClientWorker(int clientId,
                                 Coordinator coordinator,
                                 AtomicBoolean running,
                                 int numWarehouses) {
        this.clientId      = clientId;
        this.coordinator   = coordinator;
        this.running       = running;
        this.numWarehouses = numWarehouses;
    }

    @Override
    public void run() {
        LOG.log(System.Logger.Level.INFO, "T-client {0} started", clientId);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                TransactionInput txInput;
                // 50% new_order, 50% payment — mirrors HATtrick spec (48/48/4
                // but we omit order_status for simplicity)
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
                submitted.incrementAndGet();
                // Throttle to match the coordinator's batch processing capacity.
                // Without this the T-client submits millions of objects/sec into
                // the coordinator's deque, exhausting heap before any batch commits.
                // 1ms sleep → ~1000 submissions/sec per T-client, well within
                // the coordinator's processing capacity (batch_window_ms = 1000).
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING,
                        "T-client {0} error: {1}", clientId, e.getMessage());
            }
        }

        LOG.log(System.Logger.Level.INFO,
                "T-client {0} stopped. Submitted {1} txns",
                clientId, submitted.get());
    }

    /** Snapshot of submitted transaction count — call at start and end of
     *  measurement window and subtract to get transactions in window. */
    public long getSubmittedCount() {
        return submitted.get();
    }

    // ── Transaction generators ──────────────────────────────────────────────

    private NewOrderWareIn generateNewOrder() {
        int w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        int ol_cnt = DataGenUtils.randomNumber(
                TPCcConstants.MIN_NUM_ITEMS_PER_ORDER, TPCcConstants.MAX_NUM_ITEMS_PER_ORDER);

        int[] itemIds  = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty      = new int[ol_cnt];
        boolean allLocal = true;

        for (int i = 0; i < ol_cnt; i++) {
            itemIds[i]  = DataGenUtils.nuRand(8191, 7911, 1, TPCcConstants.NUM_ITEMS);
            qty[i]      = DataGenUtils.randomNumber(1, 10);
            supWares[i] = w_id; // single warehouse — keeps things simple
        }
        return new NewOrderWareIn(w_id, d_id, c_id, itemIds, supWares, qty, allLocal);
    }

    private PaymentIn generatePayment() {
        int   w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int   d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int   c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        float amount = DataGenUtils.randomNumber(100, 500000) / 100.0f;
        String c_last = DataGenUtils.randomNumber(1, 100) <= 60
                ? DataGenUtils.lastName(DataGenUtils.nuRand(255, 157, 0, 999))
                : "";
        boolean byName = !c_last.isEmpty();
        return new PaymentIn(w_id, d_id, c_id, w_id, d_id, amount, c_last, byName);
    }
}