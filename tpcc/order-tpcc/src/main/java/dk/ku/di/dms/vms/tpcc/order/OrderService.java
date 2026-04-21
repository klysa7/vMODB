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

    // ─────────────────────────────────────────────────────────────────────────
    // EVICTION STRATEGY — Table Size Target: ~350K rows per CHQ6 scan
    //
    // Per-(w_id, d_id) AtomicInteger counter. Each processNewOrder claims the
    // next ol_o_id for its district. Eviction is skipped while the counter
    // sits below ORDERS_PER_DISTRICT, then fires every transaction after that,
    // deleting the (w_id, d_id, evictOId) rows from orders, new_orders, and
    // order_line in the same write transaction.
    //
    // ORDERS_PER_DISTRICT = 4500  (paired with HATtrick ol_cnt = 3)
    //   Tuned against prior observation: at guard=6000 with ol_cnt=3, the
    //   CHQ6 scan stabilised around 390K rows. Each drop of 1000 in the
    //   guard removes ~1000 × 10 districts × 3 ol = ~30K rows from the
    //   scan. Dropping 6000 → 4500 removes ~45K, landing at ~345K — right
    //   on the 350K target.
    //
    //   Expected steady-state CHQ6 scan result:
    //     populate visible to predicates (~210K, inferred from 390K scan
    //     minus 180K net tx contribution in the guard=6000 run) plus
    //     4500 × 10 × 3 = 135K tx-created rows = ~345K total.
    //
    // DELETE LOOP SIZING
    //   Loop bound = in.itemsIds.length (the current tx's ol_cnt = 3).
    //   Since the eviction counter starts at 1 and eviction only fires past
    //   the guard (counter > 4500), the first order evicted has o_id=4501,
    //   which is well inside the tx-created range (tx orders start at
    //   d_next_o_id ≈ 3001). Populate orders (o_id 1-3000) are never
    //   targeted by eviction — they remain and form the bulk of the CHQ6
    //   scan result.
    //
    // THREAD SAFETY
    //   AtomicInteger — concurrent @Parallel transactions on the same district
    //   each evict a distinct ol_o_id. No two transactions target the same PK.
    // ─────────────────────────────────────────────────────────────────────────
    private static final int ORDERS_PER_DISTRICT = 4_500;

    private final ConcurrentHashMap<Integer, AtomicInteger> evictCounters =
            new ConcurrentHashMap<>();

    private int nextEvictOId(int w_id, int d_id) {
        int key = (w_id << 16) | d_id;
        return evictCounters
                .computeIfAbsent(key, k -> new AtomicInteger(1))
                .getAndIncrement();
    }

    // ── Runtime size tracking (INFO log every 10s) ───────────────────────────
    private static final AtomicLong TOTAL_INSERTS          = new AtomicLong(0);
    private static final AtomicLong TOTAL_DELETES          = new AtomicLong(0);
    private static final AtomicLong EVICTIONS_PERFORMED    = new AtomicLong(0);
    private static volatile long lastLogMs = 0;
    private static final long LOG_INTERVAL_MS = 10_000;

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

    @Inbound(values = "new-order-inv-out")
    @Outbound("new-order-out")
    @Transactional(type = W)
    @Parallel
    public NewOrderOut processNewOrder(NewOrderInvOut in) {
        final int olCnt = in.itemsIds.length;

        // ── INSERT: new Order + NewOrder + OrderLine rows ────────────────────
        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                new Date(),
                -1,
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

        // ── EVICT: oldest (w_id, d_id, evictOId) across all three tables ─────
        int evictOId = nextEvictOId(in.w_id, in.d_id);
        if (evictOId > ORDERS_PER_DISTRICT) {
            this.orderRepository.delete(
                    new Order(evictOId, in.d_id, in.w_id, 0, null, 0, 0, 0));

            this.newOrderRepository.delete(
                    new NewOrder(evictOId, in.d_id, in.w_id));

            for (int olNum = 1; olNum <= olCnt; olNum++) {
                this.orderLineRepository.delete(
                        new OrderLine(evictOId, in.d_id, in.w_id, olNum,
                                0, 0, null, 0, 0f, null));
            }
            TOTAL_DELETES.addAndGet(olCnt);
            EVICTIONS_PERFORMED.incrementAndGet();
        }

        // ── Periodic size log (every 10s) ────────────────────────────────────
        long nowMs = System.currentTimeMillis();
        if (nowMs - lastLogMs >= LOG_INTERVAL_MS) {
            lastLogMs = nowMs;
            long inserted  = TOTAL_INSERTS.get();
            long deleted   = TOTAL_DELETES.get();
            long evictions = EVICTIONS_PERFORMED.get();
            long net       = inserted - deleted;
            LOGGER.log(INFO,
                    "[order_line size] tx_inserted={0}, tx_deleted={1}, " +
                            "evictions_performed={2}, net={3}. " +
                            "Target: ~350K rows per CHQ6 scan with ORDERS_PER_DISTRICT={4}.",
                    inserted, deleted, evictions, net, ORDERS_PER_DISTRICT);
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