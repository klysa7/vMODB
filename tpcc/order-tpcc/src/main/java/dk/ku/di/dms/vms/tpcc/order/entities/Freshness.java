package dk.ku.di.dms.vms.tpcc.order.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * HATtrick FRESHNESS table — one instance per T-client.
 * <p>
 * Lives in the order VMS because it is the terminal VMS of both the
 * New Order DAG (warehouse → inventory → order) and the Payment DAG
 * (warehouse → order). Placing it here means the freshness counter
 * is incremented in the same process that commits the transaction,
 * eliminating any additional network round-trips.
 * <p>
 * Each T-client j owns exactly one row (client_id = j, txnnum = k).
 * After every committed transaction, the T-client sends a PATCH to
 * the order VMS to increment its txnnum. Analytical queries cross-join
 * all FRESHNESS rows and return the txnnum values alongside the result.
 * Because the query runs under snapshot isolation, the returned txnnums
 * reflect exactly which transactions were visible in the snapshot.
 */
@Entity
@VmsTable(name = "freshness")
public final class Freshness implements IEntity<Integer> {

    /** Identifies which T-client owns this row. Values: 1, 2, ... τ_max. */
    @Id
    public int client_id;

    /**
     * Monotonically increasing counter. Incremented by 1 after every
     * committed transaction from T-client {@code client_id}.
     * <p>
     * Freshness score computation:
     *   f_Aq = max(0, t_query_start - commit_time_of_first_not_seen_txn)
     * where "first not seen" = the first txnnum > returned_txnnum
     * that committed before t_query_start.
     */
    @Column
    public long txnnum;

    @SuppressWarnings("unused")
    public Freshness() {}

    public Freshness(int client_id, long txnnum) {
        this.client_id = client_id;
        this.txnnum    = txnnum;
    }

    @Override
    public String toString() {
        return "{\"client_id\":" + client_id + ",\"txnnum\":" + txnnum + "}";
    }
}