package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentIn;

/**
 * Abstraction over the vMODB coordinator for HATtrick T-clients.
 *
 * txnnum is now passed into fire* so the coordinator can enqueue a
 * PendingEntry and resolve it to a real CommitRecord when the batch commits.
 *
 * setFreshnessTracker() is called once at the start of each experiment
 * point so the per-run FreshnessTracker receives the correct commit times.
 */
public interface TransactionCoordinator {

    void fireNewOrder(NewOrderWareIn event, int clientId, long txnnum) throws Exception;

    void firePayment(PaymentIn event, int clientId, long txnnum) throws Exception;

    /** Called by HATtrickRunner at the start of each experiment point. */
    void setFreshnessTracker(FreshnessTracker tracker);
}