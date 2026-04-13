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

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

@Microservice("order")
public final class OrderService {

    private static final System.Logger LOGGER = System.getLogger(OrderService.class.getName());

    // ─────────────────────────────────────────────────────────────────────────
    // EVICTION COUNTERS — Table Size Stabilization
    //
    // Tracks the next ol_o_id to evict per (w_id, d_id) combination.
    // Key: (w_id << 16) | d_id    Value: next ol_o_id to delete (starts at 1)
    //
    // After populate, ol_o_id ranges from 1..3000 per district.
    // Counter starts at 1. Each processNewOrder atomically increments it
    // and deletes all ol_numbers (1..15) for that ol_o_id.
    // Deletes for non-existent ol_numbers are silent no-ops (safe).
    //
    // WHY NOT A QUERY:
    //   "ORDER BY ol_o_id ASC LIMIT 15" returned ALL matching rows instead of 15
    //   due to a bug in the VMS FullScanWithOrder operator's LIMIT handling.
    //   This wiped entire districts per transaction, crashing revenue.
    //   Counter approach bypasses the query entirely — pure key-based deletes,
    //   O(15) per transaction, zero scan overhead.
    //
    // THREAD SAFETY: AtomicInteger — concurrent @Parallel transactions on the
    //   same district each evict a DIFFERENT order. No two transactions clash.
    // ─────────────────────────────────────────────────────────────────────────
    private static final int MAX_OL_NUMBER = 15; // TPC-C spec: ol_count in [5,15]

    private final ConcurrentHashMap<Integer, AtomicInteger> evictCounters =
            new ConcurrentHashMap<>();

    private int nextEvictOId(int w_id, int d_id) {
        int key = (w_id << 16) | d_id;
        return evictCounters
                .computeIfAbsent(key, k -> new AtomicInteger(1))
                .getAndIncrement();
    }

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

        // ── EVICTION: delete oldest order for this (w_id, d_id) ──────────────
        // Atomically claim the next ol_o_id to evict for this district.
        // Attempt delete for ol_number 1..15 — missing rows are silent no-ops.
        int evictOId = nextEvictOId(in.w_id, in.d_id);
        for (int olNum = 1; olNum <= MAX_OL_NUMBER; olNum++) {
            this.orderLineRepository.delete(
                    new OrderLine(evictOId, in.d_id, in.w_id, olNum,
                            0, 0, null, 0, 0f, null));
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