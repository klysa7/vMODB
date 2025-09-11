package dk.ku.di.dms.vms.tpcc.order.infra;

import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.sdk.embed.client.DefaultHttpHandler;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

public final class OrderHttpHandler extends DefaultHttpHandler {

    private final IOrderRepository orderRepository;

    public OrderHttpHandler(ITransactionManager transactionManager,
                            IOrderRepository orderRepository) {
        super(transactionManager);
        this.orderRepository = orderRepository;
    }

    @Override
    public Object getAsJson(String uri) throws RuntimeException {
        String[] uriSplit = uri.split("/");
        String table = uriSplit.length > 1 ? uriSplit[1] : "";
        switch (table){
            case "order" -> {
                int orderId = Integer.parseInt(uriSplit[uriSplit.length - 3]);
                int distId = Integer.parseInt(uriSplit[uriSplit.length - 2]);
                int wareId = Integer.parseInt(uriSplit[uriSplit.length - 1]);
                this.transactionManager.beginTransaction(0, 0, 0, true);
                return this.orderRepository.lookupByKey(new Order.OrderId(orderId, distId, wareId));
            }
            case null, default -> {
                LOGGER.log(System.Logger.Level.WARNING, "URI not recognized: "+uri);
                return "";
            }
        }
    }

}
