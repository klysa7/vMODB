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
 * Workload: 50% new_order + 50% payment.
 *
 * Backpressure: uses a SHARED submitted counter across all T-client workers.
 * When τ=2, both workers check the same counter against committed — so the
 * combined in-flight never exceeds MAX_IN_FLIGHT. Without sharing, each worker
 * independently allows MAX_IN_FLIGHT, doubling the backlog and causing severe
 * coordinator contention.
 */
public final class HATtrickTClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickTClientWorker.class.getName());

    // Match num_max_transactions_batch in app.properties.
    private static final int MAX_IN_FLIGHT = 5_000;

    private final int           clientId;
    private final Coordinator   coordinator;
    private final AtomicBoolean running;
    private final int           numWarehouses;

    // Shared across all T-client workers in the same grid point.
    // All workers increment this counter and check it against committed.
    // This ensures the combined in-flight never exceeds MAX_IN_FLIGHT.
    private final AtomicLong sharedSubmitted;

    // Per-worker counter for logging only
    private final AtomicLong ownSubmitted = new AtomicLong(0L);
    private long lastPrintAt = 0;

    /**
     * Constructor with shared counter — use this when τ > 1.
     * All workers in the same grid point must share the same sharedSubmitted instance.
     */
    public HATtrickTClientWorker(int clientId,
                                 Coordinator coordinator,
                                 AtomicBoolean running,
                                 int numWarehouses,
                                 AtomicLong sharedSubmitted) {
        this.clientId        = clientId;
        this.coordinator     = coordinator;
        this.running         = running;
        this.numWarehouses   = numWarehouses;
        this.sharedSubmitted = sharedSubmitted;
    }

    /**
     * Backwards-compatible constructor — creates its own counter.
     * Safe for τ=1 (single worker). For τ>1 use the shared-counter constructor.
     */
    public HATtrickTClientWorker(int clientId,
                                 Coordinator coordinator,
                                 AtomicBoolean running,
                                 int numWarehouses) {
        this(clientId, coordinator, running, numWarehouses, new AtomicLong(0L));
    }

    @Override
    public void run() {
        LOG.log(System.Logger.Level.INFO, "T-client {0} started", clientId);

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // Backpressure: check SHARED submitted vs committed.
                // All workers contribute to sharedSubmitted, so the total
                // combined in-flight across all workers never exceeds MAX_IN_FLIGHT.
                long inFlight = sharedSubmitted.get()
                        - coordinator.getNumTIDsCommitted();
                if (inFlight > MAX_IN_FLIGHT) {
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
                sharedSubmitted.incrementAndGet();
                ownSubmitted.incrementAndGet();

                long now = System.currentTimeMillis();
                if (now - lastPrintAt >= 5000) {
                    System.out.printf("[T-client %d] Submitted %,d transactions total%n",
                            clientId, ownSubmitted.get());
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
                clientId, ownSubmitted.get());
    }

    public long getSubmittedCount() {
        return ownSubmitted.get();
    }

    private NewOrderWareIn generateNewOrder() {
        int w_id   = DataGenUtils.randomNumber(1, numWarehouses);
        int d_id   = DataGenUtils.randomNumber(1, TPCcConstants.NUM_DIST_PER_WARE);
        int c_id   = DataGenUtils.nuRand(1023, 259, 1, TPCcConstants.NUM_CUST_PER_DIST);
        int ol_cnt = 3;

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