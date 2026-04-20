package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Outbound;
import dk.ku.di.dms.vms.modb.api.annotations.Parallel;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderOut;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusOut;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentOut;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.History;
import dk.ku.di.dms.vms.tpcc.order.entities.NewOrder;
import dk.ku.di.dms.vms.tpcc.order.entities.Order;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

@Microservice("order")
public final class OrderService {

    private static final System.Logger LOGGER = System.getLogger(OrderService.class.getName());

    // Populate seeds exactly 3,000 orders per district (o_id 1..3000).
    // Guard: only evict when evictOid > 2 * ORDERS_PER_DISTRICT = 6,000,
    // so we never touch populate-inserted rows (which bypass the secondary index).
    private static final int ORDERS_PER_DISTRICT     = 3_000;
    private static final int EVICTION_SAFE_THRESHOLD = 2 * ORDERS_PER_DISTRICT; // 6,000

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository historyRepository;

    public OrderService(IOrderRepository orderRepository,
                        INewOrderRepository newOrderRepository,
                        IOrderLineRepository orderLineRepository,
                        IHistoryRepository historyRepository) {
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository = historyRepository;
    }

    @Inbound(values = "payment-out")
    @Transactional(type = W)
    @Parallel
    public void processPayment(PaymentOut out) {
        History history = new History(
                out.c_id, out.c_d_id, out.c_w_id,
                out.d_id, out.w_id,
                new Date(), out.amount, out.data);
        this.historyRepository.insert(history);
    }

    @Inbound(values = "order-status-out")
    @Transactional(type = R)
    public void processOrderStatus(OrderStatusOut in) {
        Order order = this.orderRepository.getLastOrderByCustomerId(in.c_id);
        if (order == null) {
            LOGGER.log(DEBUG, "No order found for customer " + in.c_id + "\n" + in);
            return;
        }
        List<OrderLineInfoDto> orderLinesInfo = this.orderLineRepository.getOrderLinesInfo(
                order.o_id, order.o_d_id, order.o_w_id);
        if (orderLinesInfo.isEmpty()) {
            LOGGER.log(ERROR, "Input event OrderStatusOut led to empty order lines info:\n" + in);
        }
    }

    /**
     * Processes a new order and emits NewOrderOut to the Replica VMS.
     *
     * DAG: warehouse -> inventory -> order (internal) -> replica (terminal)
     *
     * Eviction:
     *   Populate inserts 3,000 orders per district (o_id 1..3,000) bypassing
     *   the VmsForeignKey secondary index. Deleting those rows crashes vMODB.
     *   Guard: only evict when evictOid > EVICTION_SAFE_THRESHOLD (6,000),
     *   meaning we only delete rows that were inserted via normal MVCC
     *   (o_id > 6,000 per district) - never populate-inserted rows.
     *
     *   Table lifecycle: grows 300K -> ~390K during first cycle,
     *   then stabilizes at ~90K (3,000 orders x 10 districts x ol_cnt rows).
     */
    @Inbound(values = "new-order-inv-out")
    @Outbound("new-order-out")
    @Transactional(type = W)
    @Parallel
    public NewOrderOut processNewOrder(NewOrderInvOut in) {
        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                new Date(),
                -1,
                in.itemsIds.length,
                in.allLocal ? 1 : 0
        );
        NewOrder newOrder = new NewOrder(in.d_next_o_id, in.d_id, in.w_id);

        this.orderRepository.insert(order);
        this.newOrderRepository.insert(newOrder);

        List<OrderLine> orderLinesToInsert = new ArrayList<>(in.itemsIds.length);
        float[] ol_amounts = new float[in.itemsIds.length];

        for (int i = 0; i < in.itemsIds.length; i++) {
            float ol_amount = (float) (in.qty[i] * in.itemsIds[i]
                    * (1 + in.w_tax + in.d_tax) * (1 - in.c_discount));
            ol_amounts[i] = ol_amount;
            OrderLine orderLine = new OrderLine(
                    in.d_next_o_id,
                    in.d_id,
                    in.w_id,
                    i + 1,
                    in.itemsIds[i],
                    in.supWares[i],
                    null,
                    in.qty[i],
                    ol_amount,
                    in.ol_dist_info[i]
            );
            orderLinesToInsert.add(i, orderLine);
        }
        this.orderLineRepository.insertAll(orderLinesToInsert);

        // Safe eviction: only delete rows inserted via MVCC (never populate rows).
        int evictOid = in.d_next_o_id - ORDERS_PER_DISTRICT;
        if (evictOid > EVICTION_SAFE_THRESHOLD) {
            for (int ol = 1; ol <= in.itemsIds.length; ol++) {
                this.orderLineRepository.delete(
                        new OrderLine(evictOid, in.d_id, in.w_id, ol,
                                0, 0, null, 0, 0f, ""));
            }
        }

        return new NewOrderOut(
                in.w_id,
                in.d_id,
                in.d_next_o_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                ol_amounts,
                in.ol_dist_info
        );
    }
}