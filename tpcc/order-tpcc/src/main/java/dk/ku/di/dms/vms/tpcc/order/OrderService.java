package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.order.HATtrickCounters;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusOut;
import dk.ku.di.dms.vms.tpcc.common.events.PaymentOut;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderInfoDto;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.*;
import dk.ku.di.dms.vms.tpcc.order.repositories.*;

import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

@Microservice("order")
public final class OrderService {

    private final IOrderRepository     orderRepository;
    private final INewOrderRepository  newOrderRepository;
    private final IOrderLineRepository orderLineRepository;
    private final IHistoryRepository   historyRepository;

    // ── HATtrick addition ────────────────────────────────────────────────────
    private final IFreshnessRepository freshnessRepository;
    // ─────────────────────────────────────────────────────────────────────────

    public OrderService(IOrderRepository     orderRepository,
                        INewOrderRepository  newOrderRepository,
                        IOrderLineRepository orderLineRepository,
                        IHistoryRepository   historyRepository,
                        IFreshnessRepository freshnessRepository) {
        this.orderRepository     = orderRepository;
        this.newOrderRepository  = newOrderRepository;
        this.orderLineRepository = orderLineRepository;
        this.historyRepository   = historyRepository;
        this.freshnessRepository = freshnessRepository;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // New Order — terminal handler (no @Outbound)
    // ─────────────────────────────────────────────────────────────────────────

    @Inbound(values = "new-order-inv-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderInvOut in) {

        Date now = new Date();

        // Insert the order header
        Order order = new Order(
                in.d_next_o_id,
                in.d_id,
                in.w_id,
                in.c_id,
                now,
                0,                          // o_carrier_id — not assigned yet
                in.itemsIds.length,
                in.allLocal ? 1 : 0
        );
        this.orderRepository.insert(order);

        // Insert new_orders entry
        NewOrder newOrder = new NewOrder(in.d_next_o_id, in.d_id, in.w_id);
        this.newOrderRepository.insert(newOrder);

        // Insert one order_line row per item
        for (int i = 0; i < in.itemsIds.length; i++) {
            float amount = in.qty[i] * in.prices[i]
                    * (1.0f - in.c_discount)
                    * (float) (1.0 + in.w_tax + in.d_tax);

            OrderLine ol = new OrderLine(
                    in.d_next_o_id,
                    in.d_id,
                    in.w_id,
                    i + 1,              // ol_number (1-indexed)
                    in.itemsIds[i],
                    in.supWares[i],
                    null,               // ol_delivery_d — null until delivery
                    in.qty[i],
                    amount,
                    in.ol_dist_info[i]
            );
            this.orderLineRepository.insert(ol);
        }

        // ── HATtrick counters (lock-free, no transaction context needed) ──
        HATtrickCounters.ordersCount.incrementAndGet();
        HATtrickCounters.orderLineCount.addAndGet(in.itemsIds.length);

        // ── HATtrick: update freshness counter for this T-client ──────────
        // client_id == 0 means the event came from WorkloadUtils (legacy path),
        // which has no FRESHNESS row — skip the update safely.
        if (in.client_id > 0) {
            Freshness f = this.freshnessRepository.lookupByKey(in.client_id);
            if (f != null) {
                f.txnnum++;
                this.freshnessRepository.update(f);
                HATtrickCounters.setFreshness(in.client_id, f.txnnum);
            }
        }
        // ─────────────────────────────────────────────────────────────────
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Payment — terminal handler (no @Outbound)
    // ─────────────────────────────────────────────────────────────────────────

    @Inbound(values = "payment-out")
    @Transactional(type = W)
    public void processPayment(PaymentOut in) {

        History history = new History(
                in.c_id,
                in.c_d_id,
                in.c_w_id,
                in.d_id,
                in.w_id,
                new Date(),
                in.amount,
                in.data
        );
        this.historyRepository.insert(history);

        // ── HATtrick counters ─────────────────────────────────────────────
        HATtrickCounters.historyCount.incrementAndGet();

        // ── HATtrick: update freshness counter for this T-client ──────────
        if (in.client_id > 0) {
            Freshness f = this.freshnessRepository.lookupByKey(in.client_id);
            if (f != null) {
                f.txnnum++;
                this.freshnessRepository.update(f);
                HATtrickCounters.setFreshness(in.client_id, f.txnnum);
            }
        }
        // ─────────────────────────────────────────────────────────────────
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Order Status — read-only, no freshness update needed
    // ─────────────────────────────────────────────────────────────────────────

    @Inbound(values = "order-status-out")
    @Transactional(type = RW)
    public void processOrderStatus(OrderStatusOut in) {

        Order lastOrder = this.orderRepository.getLastOrderByCustomerId(in.c_id);
        if (lastOrder == null) return;

        OrderInfoDto info = this.orderRepository.getOrderInfo(
                lastOrder.o_id, lastOrder.o_d_id, lastOrder.o_w_id, in.c_id
        );

        List<OrderLineInfoDto> lines = this.orderLineRepository.getOrderLinesInfo(
                lastOrder.o_id, in.d_id, lastOrder.o_w_id
        );

        // Results are returned to the caller via the HTTP response path;
        // no outbound event needed for order status.
    }
}