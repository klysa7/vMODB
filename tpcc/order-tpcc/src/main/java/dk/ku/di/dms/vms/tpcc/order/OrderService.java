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
    // EVICTION STRATEGY — Steady-state target ~300K rows per CHQ6 scan
    //
    // Two-phase per-(w_id, d_id) FIFO eviction. Each district has an
    // AtomicInteger evictionFloor starting at 1. Every processNewOrder
    // claims floor.getAndIncrement() — the OLDEST unevicted o_id — and
    // deletes that order from orders, new_orders, and order_line.
    //
    // Phase 1 — counter ∈ [1, 3000]: populate orders are evicted.
    //   These are the rows seeded by populateDisk() in OrderHttpHandler.
    //   They have ol_count ∈ [5, 15], so we delete ol_number 1..15
    //   defensively (delete-by-key on a non-existent key is a silent no-op
    //   in vMODB, so the extra deletes for orders with ol_count < 15 cost
    //   nothing semantically).
    //
    // Phase 2 — counter > 3000: tx-created orders are evicted.
    //   Tx orders have ol_count = in.itemsIds.length (= 3 with HATtrick's
    //   ol_cnt = 3), so delete-loop stops at olCnt for those.
    //
    // PACING — eviction fires from the FIRST transaction onward, 1:1 with
    // inserts. Since populate seeds 3000 orders/district × 10 districts =
    // 30K orders × ~10 ol = ~300K rows, and every new tx replaces one
    // populate row, the table size stays anchored at ~300K throughout the
    // run.
    //
    // ROLLBACK — we don't roll back the floor on transaction abort. A
    // failed tx that did getAndIncrement() will leave a "skipped" o_id
    // that's never evicted. With at most a handful of aborts per run, the
    // drift is in the tens of rows out of 300K — invisible at this scale.
    //
    // THREAD SAFETY — AtomicInteger.getAndIncrement is atomic per district,
    // so concurrent @Parallel transactions on the same (w_id, d_id) each
    // claim a distinct evictOId.
    // ─────────────────────────────────────────────────────────────────────────
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

    // ── Runtime size tracking (INFO log every 10s) ───────────────────────────
    private static final AtomicLong TOTAL_INSERTS          = new AtomicLong(0);
    private static final AtomicLong TOTAL_DELETES          = new AtomicLong(0);
    private static final AtomicLong EVICTIONS_PERFORMED    = new AtomicLong(0);
    private static volatile long lastLogMs = 0;
    private static final long LOG_INTERVAL_MS = 10_000;

    // ─────────────────────────────────────────────────────────────────────────
    // HISTORY EVICTION — Sliding-window FIFO for the HISTORY table
    //
    // HISTORY is keyed by an @Id @GeneratedValue Integer that grows
    // monotonically from 1 (with occasional gaps from aborted or
    // interleaved transactions — observed in the SEVERE log: ...704576,
    // 704577, 704580, 704581...). Unlike orders/order_line, HISTORY is
    // NOT seeded by populate, so there is no Phase 1 — every entry comes
    // from a Payment transaction.
    //
    // PROBLEM observed at batch_sleep_ms=250: the table is pre-allocated
    // for 500K rows (max_records.history in Main.java), but at ~9K
    // payments/sec (48% of 19,500 tps × 30s × multiple cells), one grid
    // run accumulates 700K+ rows and the underlying UniqueHashBufferIndex
    // logs SEVERE "Cannot find an empty entry for history", followed by
    // JVM OOM as aborted transactions accumulate in flight buffers.
    //
    // STRATEGY — sliding window of the last HISTORY_WINDOW inserts. Once
    // we've inserted more than HISTORY_WINDOW rows, every new insert
    // triggers a delete of the oldest ID (1, 2, 3, ...). Missing IDs from
    // gaps are silent no-ops — same property the orders eviction relies
    // on. Steady-state table size ≈ HISTORY_WINDOW rows.
    //
    // SIZING — HISTORY_WINDOW = 100K rows. At ~9K payments/sec this is
    // ~11 seconds of recent history, which exceeds chq6's snapshot age
    // by an order of magnitude. The window is small enough to leave 4×
    // headroom in the 500K-row hash buffer for in-flight inserts.
    //
    // THREAD SAFETY — single AtomicLong floor since IDs are globally
    // unique (not per-district like orders). HISTORY_INSERT_COUNT and
    // HISTORY_EVICT_FLOOR are independent AtomicLongs; minor drift
    // between them under concurrency is harmless.
    // ─────────────────────────────────────────────────────────────────────────
    private static final long HISTORY_WINDOW = 100_000L;
    private static final AtomicLong HISTORY_INSERT_COUNT = new AtomicLong(0);
    private static final AtomicLong HISTORY_EVICT_FLOOR  = new AtomicLong(0);
    private static final AtomicLong HISTORY_EVICTIONS    = new AtomicLong(0);
    private static volatile long lastHistoryLogMs = 0;

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
        long inserted = HISTORY_INSERT_COUNT.incrementAndGet();

        // ── EVICT: sliding-window FIFO ───────────────────────────────────────
        // Once we've buffered HISTORY_WINDOW rows, every new insert evicts
        // the oldest. Missing IDs from gaps in @GeneratedValue (visible in
        // the SEVERE log as non-consecutive inserts) are silent no-ops, so
        // the floor advances safely without bookkeeping.
        if (inserted > HISTORY_WINDOW) {
            long evictId = HISTORY_EVICT_FLOOR.incrementAndGet();
            History toEvict = new History();
            toEvict.id = (int) evictId;
            this.historyRepository.delete(toEvict);
            HISTORY_EVICTIONS.incrementAndGet();
        }

        // ── Periodic size log (every 10s) ────────────────────────────────────
        long nowMs = System.currentTimeMillis();
        if (nowMs - lastHistoryLogMs >= LOG_INTERVAL_MS) {
            lastHistoryLogMs = nowMs;
            long ins = HISTORY_INSERT_COUNT.get();
            long ev  = HISTORY_EVICTIONS.get();
            long net = ins - ev;
            LOGGER.log(INFO,
                    "[history size] inserted={0}, evicted={1}, net={2}. " +
                            "Target: ~{3} rows steady-state (sliding-window FIFO).",
                    ins, ev, net, HISTORY_WINDOW);
        }
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
        // Floor advances 1, 2, 3, ... — strictly oldest-first.
        // Phase 1 (evictOId ≤ 3000): we're evicting populate rows.
        //   Populate orders have ol_count ∈ [5, 15], so we issue deletes for
        //   ol_number 1..15. Deletes for non-existent (o_id, ol_number) keys
        //   are silent no-ops — vMODB checks the index and skips missing rows.
        // Phase 2 (evictOId > 3000): we're evicting tx-created rows.
        //   These have ol_count = olCnt (= 3 with HATtrick), so we limit the
        //   delete loop to olCnt to avoid unnecessary index probes.
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
                            "Target: ~300K rows steady-state (FIFO, oldest-first).",
                    inserted, deleted, evictions, net);
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