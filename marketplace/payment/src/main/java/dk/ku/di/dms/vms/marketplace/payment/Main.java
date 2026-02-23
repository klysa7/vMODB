package dk.ku.di.dms.vms.marketplace.payment;

import dk.ku.di.dms.vms.marketplace.common.Constants;
import dk.ku.di.dms.vms.marketplace.payment.entities.OrderPayment;
import dk.ku.di.dms.vms.marketplace.payment.repositories.IOrderPaymentRepository;
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
                Constants.PAYMENT_VMS_PORT,
                new String[]{
                        "dk.ku.di.dms.vms.marketplace.payment",
                        "dk.ku.di.dms.vms.marketplace.common"
                }
        );

        return VmsApplication.build(options, (x, z) -> new PaymentHttpHandler(x,
                (IOrderPaymentRepository) z.apply("order_payments")));
    }

    private static class PaymentHttpHandler extends DefaultHttpHandler {

        private static final String SNAPSHOT_PARAM = "snapshot";

        private final IOrderPaymentRepository orderPaymentRepository;

        public PaymentHttpHandler(ITransactionManager transactionManager,
                                  IOrderPaymentRepository orderPaymentRepository) {
            super(transactionManager);
            this.orderPaymentRepository = orderPaymentRepository;
        }

        @Override
        public Object getAsJson(String uri) {
            if (!"/order-payments".equals(uri)) {
                return "Unknown endpoint. Use /order-payments";
            }

            //mock snapshotId to be sure
            Long requestedSnapshot = snapshotFromHeader(uri);
            long lastFinished = VMS.lastTidFinished();
            long snapshot = (requestedSnapshot != null) ? requestedSnapshot : lastFinished;
            if (snapshot > lastFinished) snapshot = lastFinished;
            this.transactionManager.beginTransaction(snapshot, 0, snapshot, true);

            List<OrderPayment> payments =
                    this.orderPaymentRepository.fetchMany(PaymentService.ORDER_PAYMENTS_ALL, OrderPayment.class);

            return orderPaymentsToJson(payments);
        }

        private static String orderPaymentsToJson(List<OrderPayment> payments) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("[");

            if (payments != null && !payments.isEmpty()) {
                for (OrderPayment p : payments) {
                    stringBuilder.append(orderPaymentToJson(p)).append(",");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }

            stringBuilder.append("]");
            return stringBuilder.toString();
        }

        private static String orderPaymentToJson(OrderPayment orderPayment) {
            if (orderPayment == null) return "null";

            return "{"
                    + "\"customer_id\":" + orderPayment.customer_id
                    + ",\"order_id\":" + orderPayment.order_id
                    + ",\"sequential\":" + orderPayment.sequential
                    + ",\"type\":" + jsonString(orderPayment.type == null ? null : orderPayment.type.name())
                    + ",\"installments\":" + orderPayment.installments
                    + ",\"value\":" + orderPayment.value
                    + ",\"status\":" + jsonString(orderPayment.status == null ? null : orderPayment.status.name())
                    + ",\"created_at\":" + jsonDate(orderPayment.created_at)
                    + "}";
        }

        private static String jsonString(String value) {
            if (value == null) return "null";
            return "\"" + value
                    .replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
                    .replace("\t", "\\t")
                    + "\"";
        }

        private static String jsonDate(Date date) {
            if (date == null) return "null";
            return String.valueOf(date.getTime());
        }

        private static Long snapshotFromHeader(String headerValue) {
            if (headerValue == null || headerValue.isBlank()) return null;
            try { return Long.parseLong(headerValue.trim()); }
            catch (NumberFormatException ignore) { return null; }
        }
    }
}