package dk.ku.di.dms.vms.tpcc.common.events;

import dk.ku.di.dms.vms.modb.api.annotations.Event;

@Event
public final class PaymentOut {

    public int w_id;
    public int d_id;
    public int c_id;

    public float amount;

    public int c_w_id;
    public int c_d_id;

    public String data;

    @SuppressWarnings("unused")
    public PaymentOut(){}

    public PaymentOut(int w_id, int d_id, int c_id, float amount, int c_w_id, int c_d_id, String data) {
        this.w_id = w_id;
        this.d_id = d_id;
        this.c_id = c_id;
        this.amount = amount;
        this.c_w_id = c_w_id;
        this.c_d_id = c_d_id;
        this.data = data;
    }

    @Override
    public String toString() {
        return "{"
                + "\"w_id\":\"" + w_id + "\""
                + ",\"d_id\":\"" + d_id + "\""
                + ",\"c_id\":\"" + c_id + "\""
//                + ",\"c_last\":\"" + c_last + "\""
//                + ",\"by_name\":\"" + by_name + "\""
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if(o instanceof PaymentOut that) {
            if (this.w_id != that.w_id) return false;
            if (this.d_id != that.d_id) return false;
            if (this.c_id != that.c_id) return false;
//            if (!this.c_last.equals(that.c_last)) return false;
//            return this.by_name == that.by_name;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = this.w_id;
        result = 31 * result + this.d_id;
        result = 31 * result + this.c_id;
//        result = 31 * result + this.c_last.hashCode();
//        result = 31 * result + (this.by_name ? 1 : 0);
        return result;
    }

}