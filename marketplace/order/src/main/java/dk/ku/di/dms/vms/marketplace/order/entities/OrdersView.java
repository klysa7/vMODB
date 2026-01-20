package dk.ku.di.dms.vms.marketplace.order.entities;

public final class OrdersView {

    public int count_orders;

    public double sum_total_invoice;

    public OrdersView() {}

    @Override
    public String toString() {
        return "{"
                + "\"count_orders\":" + count_orders
                + ",\"sum_total_invoice\":" + sum_total_invoice
                + "}";
    }
}