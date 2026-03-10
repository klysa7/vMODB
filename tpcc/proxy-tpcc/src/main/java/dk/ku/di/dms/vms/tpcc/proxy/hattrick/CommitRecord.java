package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

/**
 * Records the moment a single transaction committed from the T-client's
 * perspective. The T-client measures commit time as the instant it receives
 * the acknowledgement from the coordinator — this avoids distributed clock
 * synchronisation issues (HATtrick Section 4.2, Challenge 1).
 *
 * <pre>
 * client_id    — which T-client fired this transaction (1 … τ)
 * txnnum       — monotonic counter for this client (matches FRESHNESS_j.txnnum)
 * commitTimeNs — System.nanoTime() when the OK response arrived
 * txnType      — NEW_ORDER or PAYMENT (for per-type freshness breakdowns)
 * </pre>
 */
public final class CommitRecord {

    public enum TxnType { NEW_ORDER, PAYMENT }

    public final int     client_id;
    public final long    txnnum;
    public final long    commitTimeNs;
    public final TxnType txnType;

    public CommitRecord(int client_id, long txnnum, long commitTimeNs, TxnType txnType) {
        this.client_id    = client_id;
        this.txnnum       = txnnum;
        this.commitTimeNs = commitTimeNs;
        this.txnType      = txnType;
    }

    @Override
    public String toString() {
        return "CommitRecord{client=" + client_id
                + ", txn=" + txnnum
                + ", type=" + txnType
                + ", commitNs=" + commitTimeNs + "}";
    }
}