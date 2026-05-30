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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

@Microservice("order")
public final class OrderService {

    private static final System.Logger LOGGER = System.getLogger(OrderService.class.getName());

    private static final int POPULATE_ORDERS_PER_DISTRICT = 3_000;
    private static final int MAX_OL_NUMBER_POPULATE       = 15;

    private final ConcurrentHashMap<Integer, AtomicInteger> evictCounters =
            new ConcurrentHashMap<>();

    private int nextEvictOId(int w_id, int d_id) {
        int key = (w_id << 16) | d_id;
        return evictCounters
                .computeIfAbsent(key, k -> new AtomicInteger(1))
                .getAndIncrement();
    }

    private static final AtomicLong TOTAL_INSERTS          = new AtomicLong(0);
    private static final AtomicLong TOTAL_DELETES          = new AtomicLong(0);
    private static final AtomicLong EVICTIONS_PERFORMED    = new AtomicLong(0);
    private static volatile long lastLogMs = 0;
    private static final long LOG_INTERVAL_MS = 10_000;
    private static final long HISTORY_WINDOW = 100_000L;
    private static final AtomicLong HISTORY_INSERT_COUNT = new AtomicLong(0);
    private static final AtomicLong HISTORY_EVICT_FLOOR  = new AtomicLong(0);
    private static final AtomicLong HISTORY_EVICTIONS    = new AtomicLong(0);
    private static volatile long lastHistoryLogMs = 0;

    private final IOrderRepository orderRepository;
    private final INewOrderRepository newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository historyRepository;

    public OrderService(IOrderRepository orderRepository, INewOrderRepository newOrderRepository, IOrderLineRepository orderLineRepository, IHistoryRepository historyRepository) {
        this.orderRepository = orderRepository;
        this.newOrderRepository = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository = historyRepository;
    }

    @Inbound(values = "payment-out")
    @Transactional(type = W)
    @Parallel
    public void processPayment(PaymentOut out){
        History history = new History(out.c_id, out.c_d_id, out.c_w_id, out.d_id, out.w_id, new Date(), out.amount, out.data);
        this.historyRepository.insert(history);
        long inserted = HISTORY_INSERT_COUNT.incrementAndGet();

        if (inserted > HISTORY_WINDOW) {
            long evictId = HISTORY_EVICT_FLOOR.incrementAndGet();
            History toEvict = new History();
            toEvict.id = (int) evictId;
            this.historyRepository.delete(toEvict);
            HISTORY_EVICTIONS.incrementAndGet();
        }

//        long nowMs = System.currentTimeMillis();
//        if (nowMs - lastHistoryLogMs >= LOG_INTERVAL_MS) {
//            lastHistoryLogMs = nowMs;
//            long ins = HISTORY_INSERT_COUNT.get();
//            long ev  = HISTORY_EVICTIONS.get();
//            long net = ins - ev;
//            LOGGER.log(INFO,
//                    "[history size] inserted={0}, evicted={1}, net={2}. " +
//                            "Target: ~{3} rows steady-state (sliding-window FIFO).",
//                    ins, ev, net, HISTORY_WINDOW);
//        }
    }

    @Inbound(values = "order-status-out")
    @Transactional(type = R)
    public void processOrderStatus(OrderStatusOut in){
        Order order = this.orderRepository.getLastOrderByCustomerId(in.c_id);
        if(order == null){
            LOGGER.log(DEBUG, "No order found for customer "+in.c_id+"\n"+in);
            return;
        }
        List<OrderLineInfoDto> orderLinesInfo = this.orderLineRepository.getOrderLinesInfo(order.o_id, order.o_d_id, order.o_w_id);
        if(orderLinesInfo.isEmpty()){
            LOGGER.log(ERROR, "Input event OrderStatusOut led to empty order lines info:\n"+in);
        }
    }

    @Inbound(values = "new-order-inv-out")
    @Outbound("new-order-out")
    @Transactional(type = W)
    @Parallel
    public NewOrderOut processNewOrder(NewOrderInvOut in) {
        final int olCnt = in.itemsIds.length;

        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                new Date(),
                -1,  // set in delivery tx
                olCnt,
                in.allLocal ? 1 : 0
        );
        NewOrder newOrder = new NewOrder(in.d_next_o_id, in.d_id, in.w_id);

        this.orderRepository.insert(order);
        this.newOrderRepository.insert(newOrder);

        List<OrderLine> orderLinesToInsert = new ArrayList<>(olCnt);
        float[] ol_amounts = new float[olCnt];

        for (int i = 0; i < olCnt; i++) {
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
        TOTAL_INSERTS.addAndGet(olCnt);

        int evictOId = nextEvictOId(in.w_id, in.d_id);

        this.orderRepository.delete(
                new Order(evictOId, in.d_id, in.w_id, 0, null, 0, 0, 0));
        this.newOrderRepository.delete(
                new NewOrder(evictOId, in.d_id, in.w_id));

        int olDeleteBound = (evictOId <= POPULATE_ORDERS_PER_DISTRICT)
                ? MAX_OL_NUMBER_POPULATE
                : olCnt;
        for (int olNum = 1; olNum <= olDeleteBound; olNum++) {
            this.orderLineRepository.delete(
                    new OrderLine(evictOId, in.d_id, in.w_id, olNum,
                            0, 0, null, 0, 0f, null));
        }
        TOTAL_DELETES.addAndGet(olDeleteBound);
        EVICTIONS_PERFORMED.incrementAndGet();

//        long nowMs = System.currentTimeMillis();
//        if (nowMs - lastLogMs >= LOG_INTERVAL_MS) {
//            lastLogMs = nowMs;
//            long inserted  = TOTAL_INSERTS.get();
//            long deleted   = TOTAL_DELETES.get();
//            long evictions = EVICTIONS_PERFORMED.get();
//            long net       = inserted - deleted;
//            LOGGER.log(INFO,
//                    "[order_line size] tx_inserted={0}, tx_deleted={1}, " +
//                            "evictions_performed={2}, net={3}. " +
//                            "Target: ~300K rows steady-state (FIFO, oldest-first).",
//                    inserted, deleted, evictions, net);
//        }

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
