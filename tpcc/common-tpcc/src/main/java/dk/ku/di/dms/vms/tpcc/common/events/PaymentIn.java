package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;
import dk.ku.di.dms.vms.tpcc.common.etc.WareDistId;

import java.util.Set;

@Event
public final class PaymentIn {

    public int w_id;
    public int d_id;
    public int c_id;

    // "15% of the transactions assume the customer is paying through
    //  a warehouse other than the customer's home warehouse."
    public int c_w_id;
    public int c_d_id;

    public float amount;

    public String c_last;
    public boolean by_name;

    /**
     * HATtrick: which T-client submitted this transaction.
     * Propagated to PaymentOut so OrderService can update FRESHNESS.
     */
    public int client_id;

    @SuppressWarnings("unused")
    public PaymentIn() {}

    /** Backward-compatible constructor for WorkloadUtils / legacy call sites (client_id = 0). */
    public PaymentIn(int w_id, int d_id, int c_id, int c_w_id, int c_d_id,
                     float amount, String c_last, boolean by_name) {
        this(w_id, d_id, c_id, c_w_id, c_d_id, amount, c_last, by_name, 0);
    }

    public PaymentIn(int w_id, int d_id, int c_id, int c_w_id, int c_d_id,
                     float amount, String c_last, boolean by_name, int client_id) {
        this.w_id      = w_id;
        this.d_id      = d_id;
        this.c_id      = c_id;
        this.c_w_id    = c_w_id;
        this.c_d_id    = c_d_id;
        this.amount    = amount;
        this.c_last    = c_last;
        this.by_name   = by_name;
        this.client_id = client_id;
    }

    @SuppressWarnings("unused")
    public Set<WareDistId> getId() {
        if (this.w_id == this.c_w_id && this.d_id == this.c_d_id)
            return Set.of(new WareDistId(this.w_id, 0), new WareDistId(this.w_id, this.d_id));
        return Set.of(new WareDistId(this.w_id, 0),
                new WareDistId(this.w_id, this.d_id),
                new WareDistId(this.c_w_id, this.c_d_id));
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"c_id\":" + c_id
                + ",\"c_w_id\":" + c_w_id
                + ",\"c_d_id\":" + c_d_id
                + ",\"amount\":" + amount
                + ",\"c_last\":\"" + c_last + "\""
                + ",\"by_name\":" + by_name
                + ",\"client_id\":" + client_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof PaymentIn that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
            if (this.c_w_id != that.c_w_id) return false;
            if (this.c_d_id != that.c_d_id) return false;
            if (this.amount != that.amount) return false;
            if (!this.c_last.equals(that.c_last)) return false;
            return this.by_name == that.by_name;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        result = 31 * result + this.c_w_id;
        result = 31 * result + this.c_d_id;
        result = (int) (31 * result + this.amount);
        result = 31 * result + this.c_last.hashCode();
        result = 31 * result + (this.by_name ? 1 : 0);
        return result;
    }
}