package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick T-client thread.
 *
 * FIX FOR PROBLEM 1:
 *   No longer calls freshnessTracker.recordCommit() directly.
 *   Instead, passes txnnum to coordinator.fireXxx(), which enqueues a
 *   PendingEntry. The VmsTransactionCoordinator batch-commit callback
 *   resolves it with the actual commit timestamp.
 *
 * FIX FOR PROBLEM 5 (shutdown):
 *   The while loop checks Thread.currentThread().isInterrupted() in addition
 *   to running.get(). HATtrickRunner calls pool.shutdownNow() which sends
 *   interrupts to all threads, so workers stop within one loop iteration
 *   rather than waiting up to 30s.
 */
public final class TClientWorker implements Runnable {

    private static final System.Logger LOG = System.getLogger(TClientWorker.class.getName());

    private final int                    clientId;
    private final TransactionCoordinator coordinator;
    private final AtomicBoolean          running;
    private final int                    numWarehouses;

    private final AtomicLong txnnum       = new AtomicLong(0L);
    private volatile long    committedTxns = 0L;
    private volatile long    startTimeNs   = 0L;

    public TClientWorker(int clientId,
                         TransactionCoordinator coordinator,
                         AtomicBoolean running,
                         int numWarehouses) {
        this.clientId     = clientId;
        this.coordinator  = coordinator;
        this.running      = running;
        this.numWarehouses = numWarehouses;
    }

    @Override
    public void run() {
        startTimeNs = System.nanoTime();
        LOG.log(System.Logger.Level.INFO, "T-client {0} started", clientId);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                boolean isNewOrder = DataGenUtils.randomNumber(1, 100) <= 48;
                long num = txnnum.incrementAndGet();

                if (isNewOrder) {
                    NewOrderWareIn event = generateNewOrder();
                    coordinator.fireNewOrder(event, clientId, num);
                } else {
                    PaymentIn event = generatePayment();
                    coordinator.firePayment(event, clientId, num);
                }
                committedTxns++;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // restore flag and exit
                break;
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING,
                        "T-client {0} error: {1}", clientId, e.getMessage());
            }
        }

        LOG.log(System.Logger.Level.INFO,
                "T-client {0} stopped. Queued {1} txns in {2}s",
                clientId, committedTxns, elapsedSeconds());
    }

    public double throughputTps() {
        double e = elapsedSeconds();
        return e > 0 ? committedTxns / e : 0.0;
    }

    public long getCommittedTxns() { return committedTxns; }

    private double elapsedSeconds() {
        return (System.nanoTime() - startTimeNs) / 1e9;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Event builders
    // ─────────────────────────────────────────────────────────────────────────

    private NewOrderWareIn generateNewOrder() {
        int w_id = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        int ol_cnt = DataGenUtils.randomNumber(
                TPCcConstants.MIN_NUM_ITEMS_PER_ORDER, TPCcConstants.MAX_NUM_ITEMS_PER_ORDER);

        int[] itemsIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty      = new int[ol_cnt];
        boolean allLocal = true;

        for (int i = 0; i < ol_cnt; i++) {
            itemsIds[i] = DataGenUtils.nuRand(8191, 7911, 1, TPCcConstants.NUM_ITEMS);
            qty[i]      = DataGenUtils.randomNumber(1, 10);
            if (numWarehouses > 1 && DataGenUtils.randomNumber(1, 100) == 1) {
                supWares[i] = DataGenUtils.randomNumber(1, numWarehouses);
                if (supWares[i] == w_id) supWares[i] = (w_id % numWarehouses) + 1;
                allLocal = false;
            } else {
                supWares[i] = w_id;
            }
        }
        return new NewOrderWareIn(w_id, d_id, c_id, itemsIds, supWares, qty, allLocal, clientId);
    }

    private PaymentIn generatePayment() {
        int w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        int c_w_id = w_id;
        int c_d_id = d_id;
        if (numWarehouses > 1 && DataGenUtils.randomNumber(1, 100) <= 15) {
            c_w_id = DataGenUtils.randomNumber(1, numWarehouses);
            if (c_w_id == w_id) c_w_id = (w_id % numWarehouses) + 1;
            c_d_id = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        }
        float  amount = DataGenUtils.randomNumber(100, 500000) / 100.0f;
        String c_last = DataGenUtils.randomNumber(1, 100) <= 60
                ? DataGenUtils.lastName(DataGenUtils.nuRand(255, 157, 0, 999))
                : "";
        boolean byName = !c_last.isEmpty();
        return new PaymentIn(w_id, d_id, c_id, c_w_id, c_d_id, amount, c_last, byName, clientId);
    }
}