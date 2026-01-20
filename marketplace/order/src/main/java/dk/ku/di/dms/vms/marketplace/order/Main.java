package dk.ku.di.dms.vms.marketplace.order;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.order.entities.Order;
import dk.ku.di.dms.vms.marketplace.order.repositories.IOrderRepository;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import static java.lang.System.Logger.Level.DEBUG;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    private static VmsApplication VMS;

    public static void main(String[] args) throws Exception {
        Properties properties = ConfigUtils.loadProperties();
        VMS = initVms(properties);
        VMS.start();
    }

    private static VmsApplication initVms(Properties properties) throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                properties,
                "0.0.0.0",
                Constants.ORDER_VMS_PORT,
                new String[]{
                        "dk.ku.di.dms.vms.marketplace.order",
                        "dk.ku.di.dms.vms.marketplace.common"
                }
        );
        return VmsApplication.build(options, (x,z) -> new OrderHttpHandler(x,
                (IOrderRepository) z.apply("orders")));

    }

    private static class OrderHttpHandler extends DefaultHttpHandler {
        private final IOrderRepository orderRepository;

        private static Long snapshotFromHeader(String headerValue) {
            if (headerValue == null || headerValue.isBlank()) return null;
            try { return Long.parseLong(headerValue.trim()); }
            catch (NumberFormatException ignore) { return null; }
        }

        public OrderHttpHandler(ITransactionManager transactionManager,
                                IOrderRepository orderRepository) {
            super(transactionManager);
            this.orderRepository = orderRepository;
        }

        @Override
        public Object getAsJson(String uri) {
            if (!"/orders".equals(uri)) {
                return "Unknown endpoint. Use /orders";
            }
            //mock snapshotId to be sure
            Long requestedSnapshot = snapshotFromHeader(uri);
            long lastFinished = VMS.lastTidFinished();
            long snapshot = (requestedSnapshot != null) ? requestedSnapshot : lastFinished;
            if (snapshot > lastFinished) {
                snapshot = lastFinished;
            }
            this.transactionManager.beginTransaction(snapshot, 0, snapshot, true);
            List<Order> view = this.orderRepository.fetchMany(OrderService.ORDERS_ALL, Order.class);
            return ordersToJson(view);
        }

        private static String ordersToJson(List<Order> orders) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");

            if (orders != null && !orders.isEmpty()) {
                for (Order o : orders) {
                    stringBuilder.append(orderToJson(o)).append(",");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1); // remove last comma
            }

            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        private static String orderToJson(Order order) {
            if (order == null) return "null";

            return "{"
                    + "\"customer_id\":" + order.customer_id
                    + ",\"order_id\":" + order.order_id
                    + ",\"invoice_number\":" + jsonString(order.invoice_number)
                    + ",\"status\":" + jsonString(order.status == null ? null : order.status.name())
                    + ",\"purchase_date\":" + jsonDate(order.purchase_date)
                    + ",\"payment_date\":" + jsonDate(order.payment_date)
                    + ",\"delivered_carrier_date\":" + jsonDate(order.delivered_carrier_date)
                    + ",\"delivered_customer_date\":" + jsonDate(order.delivered_customer_date)
                    + ",\"estimated_delivery_date\":" + jsonDate(order.estimated_delivery_date)
                    + ",\"count_items\":" + order.count_items
                    + ",\"total_amount\":" + order.total_amount
                    + ",\"total_freight\":" + order.total_freight
                    + ",\"total_incentive\":" + order.total_incentive
                    + ",\"total_invoice\":" + order.total_invoice
                    + ",\"total_items\":" + order.total_items
                    + ",\"created_at\":" + jsonDate(order.created_at)
                    + ",\"updated_at\":" + jsonDate(order.updated_at)
                    + "}";
        }

        private static String jsonString(String value) {
            if (value == null) return "null";
            // minimal JSON escaping
            return "\"" + value
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t")
                    + "\"";
        }

        private static String jsonDate(Date d) {
            if (d == null) return "null";
            return String.valueOf(d.getTime());
        }
    }
}