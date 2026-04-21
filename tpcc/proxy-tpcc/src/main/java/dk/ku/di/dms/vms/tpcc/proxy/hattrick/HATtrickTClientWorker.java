package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.tpcc.common.datagen.DataGenUtils;
import dk.ku.di.dms.vms.tpcc.common.datagen.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * HATtrick T-client for the throughput frontier experiment.
 *
 * Workload: 50% new_order + 50% payment.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * CHANGE 1 — Simplified new_order (ol_cnt = 3)
 *   Was: DataGenUtils.randomNumber(5, 15) from TPC-C spec constants.
 *   Now: fixed at 3. Paired with OrderService eviction so every tx inserts
 *   exactly 3 order_line rows and (after guard) deletes exactly 3. Net 0 at
 *   steady state. This keeps the TPC-C feel (small ol_cnt within the
 *   [1, 15] legal range) while giving us a predictable insert/delete cadence
 *   that's easy to reason about when tuning eviction.
 *
 * CHANGE 2 — Removed Thread.sleep(1)
 *   The sleep was capping each client at ~1000 tps regardless of coordinator
 *   capacity. Removed so clients can sustain multi-thousand tps when the
 *   pipeline can absorb them.
 *
 * CHANGE 3 — Per-client inflight budget (maxInFlight)
 *   Replacement for the removed sleep. Before every submit we check
 *   (submitted - committed). If it exceeds the configured budget, we park
 *   the thread for 100μs and re-check.
 *
 *   The budget is passed by the caller (HATtrickRunner) as
 *   MAX_IN_FLIGHT_PER_CLIENT × τ. This gives each client the same effective
 *   per-client budget regardless of how many clients are in the point,
 *   preventing the τ=2 regression where both clients thrashed against one
 *   shared 5000-slot counter.
 *
 * CHANGE 4 — LockSupport.parkNanos instead of Thread.yield
 *   Thread.yield() is only a scheduler hint and keeps the thread hot,
 *   burning CPU that should go to the coordinator's TransactionWorker and
 *   the VMS worker threads. A 100μs park actually releases the CPU.
 * ─────────────────────────────────────────────────────────────────────────────
 */
public final class HATtrickTClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickTClientWorker.class.getName());

    /**
     * Fixed ol_cnt for all new_order transactions. Must be ≤ MAX_OL_NUMBER
     * in OrderService so eviction deletes exactly the inserted rows.
     */
    private static final int OL_CNT = 3;

    private final int           clientId;
    private final Coordinator   coordinator;
    private final AtomicBoolean running;
    private final int           numWarehouses;

    /**
     * Global inflight budget seen by this client. When
     * (submitted - committed) exceeds this, we park instead of submitting.
     * HATtrickRunner sets this to MAX_IN_FLIGHT_PER_CLIENT × τ so each
     * client gets a fair share.
     */
    private final int maxInFlight;

    private final AtomicLong submitted = new AtomicLong(0L);
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

    @Override
    public void run() {
        LOG.log(System.Logger.Level.INFO, "T-client {0} started", clientId);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // ── Backpressure ────────────────────────────────────────────
                // Park (don't spin-yield) when the coordinator queue is full.
                // parkNanos(100_000) actually releases the CPU for ~100μs,
                // letting the coordinator's TransactionWorker and VMS threads
                // make progress. With Thread.yield() we were burning CPU here
                // and starving the coordinator — visible as the τ=2 collapse
                // from 13K tps down to 845 tps.
                long inFlight = coordinator.getNumTIDsSubmitted()
                        - coordinator.getNumTIDsCommitted();
                if (inFlight > this.maxInFlight) {
                    LockSupport.parkNanos(100_000);
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
                submitted.incrementAndGet();

                long now = System.currentTimeMillis();
                if (now - lastPrintAt >= 5000) {
                    System.out.printf("[T-client %d] Submitted %,d transactions total%n",
                            clientId, submitted.get());
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
                clientId, submitted.get());
    }

    public long getSubmittedCount() {
        return submitted.get();
    }

    private NewOrderWareIn generateNewOrder() {
        int w_id = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);

        int ol_cnt = OL_CNT;

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
        // Always use by_id (by_name=false) — by-name throws "Empty customer list"
        // in WarehouseService and crashes the VMS.
        return new PaymentIn(w_id, d_id, c_id, w_id, d_id, amount, "", false);
    }
}