package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.coordinator.transaction.TransactionInput;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concrete adapter between TClientWorker and the vMODB Coordinator.
 *
 * FIX FOR PROBLEM 1 (commit time correctness):
 *
 *   fireNewOrder / firePayment no longer record commitTimeNs immediately.
 *   Instead, each call enqueues a PendingEntry(clientId, txnnum, type).
 *
 *   When the coordinator commits a batch, registerBatchCommitConsumer fires.
 *   At that point we drain pendingQueue, stamping every entry with
 *   System.nanoTime() as the TRUE commit time, and forward them to
 *   FreshnessTracker.recordCommit().
 *
 *   This gives freshness scores that measure the actual snapshot lag
 *   (batch commit window) rather than the queue submission time.
 *
 * FIX FOR PER-RUN FRESHNESS TRACKER:
 *
 *   HATtrickRunner calls setFreshnessTracker() once at the start of each
 *   experiment point. The coordinator instance is reused across runs, but
 *   each run gets a fresh FreshnessTracker.
 */
public final class VmsTransactionCoordinator implements TransactionCoordinator {

    private static final System.Logger LOG =
            System.getLogger(VmsTransactionCoordinator.class.getName());

    private final Coordinator coordinator;

    // Per-run FreshnessTracker — updated by HATtrickRunner before each point
    private final AtomicReference<FreshnessTracker> trackerRef =
            new AtomicReference<>(null);

    // Pending entries: queued by T-clients, drained on batch commit
    private record PendingEntry(int clientId, long txnnum, CommitRecord.TxnType type) {}
    private final ConcurrentLinkedQueue<PendingEntry> pendingQueue =
            new ConcurrentLinkedQueue<>();

    public VmsTransactionCoordinator(Coordinator coordinator) {
        this.coordinator = coordinator;

        // Register batch commit callback — fires when coordinator commits a batch.
        // At this point all transactions in the batch are visible in MVCC snapshots.
        coordinator.registerBatchCommitConsumer((batchOffset, lastTid) -> {
            long commitTimeNs = System.nanoTime();
            FreshnessTracker tracker = trackerRef.get();
            if (tracker == null) {
                // No experiment running — discard pending entries
                pendingQueue.clear();
                return;
            }
            PendingEntry entry;
            int count = 0;
            while ((entry = pendingQueue.poll()) != null) {
                tracker.recordCommit(new CommitRecord(
                        entry.clientId(), entry.txnnum(), commitTimeNs, entry.type()));
                count++;
            }
            if (count > 0) {
                LOG.log(System.Logger.Level.DEBUG,
                        "Batch committed: resolved {0} pending entries at t={1}ns",
                        count, commitTimeNs);
            }
        });
    }

    @Override
    public void setFreshnessTracker(FreshnessTracker tracker) {
        // Drain any leftover pending entries from the previous run before switching
        pendingQueue.clear();
        trackerRef.set(tracker);
    }

    @Override
    public void fireNewOrder(NewOrderWareIn event, int clientId, long txnnum) {
        TransactionInput.Event payload =
                new TransactionInput.Event("new-order-ware-in", event.toString());
        coordinator.queueTransactionInput(new TransactionInput("new_order", payload));
        // Enqueue AFTER queuing to coordinator so we don't resolve before submission
        if (clientId > 0) {
            pendingQueue.add(new PendingEntry(clientId, txnnum, CommitRecord.TxnType.NEW_ORDER));
        }
    }

    @Override
    public void firePayment(PaymentIn event, int clientId, long txnnum) {
        TransactionInput.Event payload =
                new TransactionInput.Event("payment-in", event.toString());
        coordinator.queueTransactionInput(new TransactionInput("payment", payload));
        if (clientId > 0) {
            pendingQueue.add(new PendingEntry(clientId, txnnum, CommitRecord.TxnType.PAYMENT));
        }
    }
}