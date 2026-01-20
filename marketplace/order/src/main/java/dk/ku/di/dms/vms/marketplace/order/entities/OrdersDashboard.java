package dk.ku.di.dms.vms.marketplace.order.entities;


import javax.persistence.criteria.Order;
import java.util.List;

public final class OrdersDashboard {

    public final OrdersView view;     // can be null for now
    public final List<Order> orders;

    public OrdersDashboard(OrdersView view, List<Order> orders) {
        this.view = view;
        this.orders = orders;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        if (orders != null && !orders.isEmpty()) {
            for (Order o : orders) {
                sb.append(o.toString()).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append("]");

        return "{"
                + "\"view\":" + (view == null ? "null" : view.toString())
                + ",\"orders\":" + sb
                + "}";
    }
}