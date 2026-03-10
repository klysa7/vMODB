package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;
import dk.ku.di.dms.vms.tpcc.common.etc.WareDistId;

import java.util.Arrays;

@Event
public final class NewOrderWareIn {

    public int w_id;
    public int d_id;
    public int c_id;
    public int[] itemsIds;
    public int[] supWares;
    public int[] qty;
    public boolean allLocal;

    /**
     * HATtrick: which T-client thread submitted this transaction.
     * Threaded through WareOut → InvOut → OrderService so the terminal
     * VMS can update the correct FRESHNESS row.
     */
    public int client_id;

    @SuppressWarnings("unused")
    public NewOrderWareIn() {}

    /** Backward-compatible constructor for WorkloadUtils / legacy call sites (client_id = 0). */
    public NewOrderWareIn(int w_id, int d_id, int c_id,
                          int[] itemsIds, int[] supWares, int[] qty,
                          boolean allLocal) {
        this(w_id, d_id, c_id, itemsIds, supWares, qty, allLocal, 0);
    }

    public NewOrderWareIn(int w_id, int d_id, int c_id,
                          int[] itemsIds, int[] supWares, int[] qty,
                          boolean allLocal, int client_id) {
        this.w_id      = w_id;
        this.d_id      = d_id;
        this.c_id      = c_id;
        this.itemsIds  = itemsIds;
        this.supWares  = supWares;
        this.qty       = qty;
        this.allLocal  = allLocal;
        this.client_id = client_id;
    }

    @SuppressWarnings("unused")
    public WareDistId getId() {
        return new WareDistId(this.w_id, 0);
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":" + w_id
                + ",\"d_id\":" + d_id
                + ",\"c_id\":" + c_id
                + ",\"itemsIds\":" + Arrays.toString(itemsIds)
                + ",\"supWares\":" + Arrays.toString(supWares)
                + ",\"qty\":" + Arrays.toString(qty)
                + ",\"allLocal\":" + allLocal
                + ",\"client_id\":" + client_id
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NewOrderWareIn that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
            if (this.allLocal != that.allLocal) return false;
            int maxSize = Math.min(this.itemsIds.length, that.itemsIds.length);
            int idx = 0;
            while (idx < maxSize) {
                if (this.itemsIds[idx] != that.itemsIds[idx]) {
                    return this.itemsIds[idx] == -1 || that.itemsIds[idx] == -1;
                }
                idx++;
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
        result = 31 * result + Arrays.hashCode(this.itemsIds);
        result = 31 * result + Arrays.hashCode(this.supWares);
        result = 31 * result + Arrays.hashCode(this.qty);
        result = 31 * result + (this.allLocal ? 1 : 0);
        return result;
    }
}