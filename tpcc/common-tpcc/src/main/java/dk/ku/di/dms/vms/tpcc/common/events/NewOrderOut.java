package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

/**
 * Emitted by the Order VMS after processing a new_order transaction.
 * Carries the pre-computed order line data so the Replica VMS can
 * insert into its own order_line table without re-computing anything.
 * This event is the output of the order VMS internal node in the
 * new_order DAG Warehouse → inventory → order (internal), replica (terminal), emits NewOrderOut
 * * Fields match OrderLine exactly so ReplicaService can insert directly.
 */
@Event
public final class NewOrderOut {

    public int    w_id;
    public int    d_id;
    public int    o_id;
    public int[]  itemsIds;
    public int[]  supWares;
    public int[]  qty;
    public float[] ol_amounts;
    public String[] ol_dist_info;

    public NewOrderOut() {}

    public NewOrderOut(int w_id, int d_id, int o_id,
                       int[] itemsIds, int[] supWares, int[] qty,
                       float[] ol_amounts, String[] ol_dist_info) {
        this.w_id        = w_id;
        this.d_id        = d_id;
        this.o_id        = o_id;
        this.itemsIds    = itemsIds;
        this.supWares    = supWares;
        this.qty         = qty;
        this.ol_amounts  = ol_amounts;
        this.ol_dist_info = ol_dist_info;
    }
}