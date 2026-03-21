package dk.ku.di.dms.vms.tpcc.common.events;

/**
 * Input event for the delete_orderline transaction.
 *
 * Carries the exact order to delete: all order_line rows matching
 * (ol_w_id = w_id, ol_d_id = d_id, ol_o_id = o_id) will be removed.
 *
 * The proxy T-client maintains a per-district counter starting at 1
 * so that the oldest populated order is deleted first, keeping the
 * order_line table at a stable size during HTAP experiments.
 */
public final class DeleteOrderLineIn {

    public final int w_id;
    public final int d_id;
    public final int o_id;

    public DeleteOrderLineIn(int w_id, int d_id, int o_id) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.o_id = o_id;
    }

    @Override
    public String toString() {
        return "{\"w_id\":" + w_id + ",\"d_id\":" + d_id + ",\"o_id\":" + o_id + "}";
    }

    public static DeleteOrderLineIn fromString(String s) {
        String[] parts = s.split(",");
        return new DeleteOrderLineIn(
                Integer.parseInt(parts[0].trim()),
                Integer.parseInt(parts[1].trim()),
                Integer.parseInt(parts[2].trim())
        );
    }
}