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
 * Table stability: the delete logic lives INSIDE processNewOrder() on
 * the order VMS — every new_order atomically deletes the oldest order
 * for the same district. No separate delete transaction needed here.
 */
public final class HATtrickTClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickTClientWorker.class.getName());

    private final int           clientId;
    private final Coordinator   coordinator;
    private final AtomicBoolean running;
    private final int           numWarehouses;

    private final AtomicLong submitted = new AtomicLong(0L);
    private long lastPrintAt = 0;

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

                // Print every 5 seconds to confirm throughput
                long now = System.currentTimeMillis();
                if (now - lastPrintAt >= 5000) {
                    System.out.printf("[T-client %d] Submitted %d transactions total%n",
                            clientId, submitted.get());
                    lastPrintAt = now;
                }

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

    public long getSubmittedCount() {
        return submitted.get();
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
        // Always use by_id (by_name=false) — the by-name lookup throws
        // "Empty customer list" in WarehouseService crashing the VMS.
        return new PaymentIn(w_id, d_id, c_id, w_id, d_id, amount, "", false);
    }
}