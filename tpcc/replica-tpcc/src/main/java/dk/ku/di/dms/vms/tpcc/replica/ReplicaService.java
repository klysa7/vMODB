package dk.ku.di.dms.vms.tpcc.replica;

import dk.ku.di.dms.vms.modb.api.annotations.Inbound;
import dk.ku.di.dms.vms.modb.api.annotations.Microservice;
import dk.ku.di.dms.vms.modb.api.annotations.Parallel;
import dk.ku.di.dms.vms.modb.api.annotations.Transactional;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderOut;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;
import dk.ku.di.dms.vms.tpcc.replica.repositories.IOrderLineReplicaRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

/**
 * Replica VMS service — terminal node in the new_order DAG.
 *
 * Tracks revenue incrementally for two queries:
 *
 * CHQ6: SELECT SUM(ol_amount) FROM order_line
 *   → single AtomicLong REVENUE_BITS (float bits)
 *
 * CHQ1: SELECT ol_number, SUM(ol_quantity), SUM(ol_amount),
 *              AVG(ol_quantity), AVG(ol_amount), COUNT(*)
 *       FROM order_line GROUP BY ol_number
 *   → 3 AtomicLong arrays of length 10 (one slot per ol_number 1-10):
 *       CHQ1_AMOUNT[i], CHQ1_QUANTITY[i], CHQ1_COUNT[i]
 *
 * All counters are updated atomically in processNewOrder() under @Parallel,
 * using CAS loops so concurrent transactions don't race.
 *
 * The HTTP handler in Main reads these counters directly — O(1) per query,
 * no MVCC snapshot needed, no lock contention with OLTP.
 */
@Microservice("replica")
public final class ReplicaService {

    // ── CHQ6: total revenue ───────────────────────────────────────────────────
    // Float bits stored in a long for atomic CAS.
    static final AtomicLong REVENUE_BITS = new AtomicLong(
            Float.floatToRawIntBits(0.0f) & 0xFFFFFFFFL);

    // ── CHQ1: per-ol_number aggregates ───────────────────────────────────────
    // TPC-C: ol_number in [1..10]. Index 0 = ol_number 1, index 9 = ol_number 10.
    // AMOUNT: float bits in long (CAS)
    // QUANTITY: plain long (atomic add)
    // COUNT: plain long (atomic increment)
    static final AtomicLong[] CHQ1_AMOUNT   = new AtomicLong[10];
    static final AtomicLong[] CHQ1_QUANTITY = new AtomicLong[10];
    static final AtomicLong[] CHQ1_COUNT    = new AtomicLong[10];

    static {
        for (int i = 0; i < 10; i++) {
            CHQ1_AMOUNT[i]   = new AtomicLong(Float.floatToRawIntBits(0.0f) & 0xFFFFFFFFL);
            CHQ1_QUANTITY[i] = new AtomicLong(0);
            CHQ1_COUNT[i]    = new AtomicLong(0);
        }
    }

    // ── CAS helpers ───────────────────────────────────────────────────────────

    static void addRevenue(float delta) {
        addFloatAtomic(REVENUE_BITS, delta);
    }

    static float getRevenue() {
        return Float.intBitsToFloat((int) REVENUE_BITS.get());
    }

    static void addChq1(int olNumber, float amount, int quantity) {
        int idx = olNumber - 1; // ol_number is 1-based
        if (idx < 0 || idx >= 10) return;
        addFloatAtomic(CHQ1_AMOUNT[idx], amount);
        CHQ1_QUANTITY[idx].addAndGet(quantity);
        CHQ1_COUNT[idx].incrementAndGet();
    }

    static float getChq1Amount(int idx) {
        return Float.intBitsToFloat((int) CHQ1_AMOUNT[idx].get());
    }

    private static void addFloatAtomic(AtomicLong target, float delta) {
        long prev, next;
        do {
            prev = target.get();
            float prevF = Float.intBitsToFloat((int) prev);
            float nextF = prevF + delta;
            next = Float.floatToRawIntBits(nextF) & 0xFFFFFFFFL;
        } while (!target.compareAndSet(prev, next));
    }

    // ── VMS service ───────────────────────────────────────────────────────────

    private final IOrderLineReplicaRepository orderLineReplicaRepository;

    public ReplicaService(IOrderLineReplicaRepository orderLineReplicaRepository) {
        this.orderLineReplicaRepository = orderLineReplicaRepository;
    }

    @Inbound(values = "new-order-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderOut in) {
        List<OrderLineReplica> toInsert = new ArrayList<>(in.itemsIds.length);
        for (int i = 0; i < in.itemsIds.length; i++) {
            int   olNumber = i + 1;
            float amount   = in.ol_amounts[i];
            int   qty      = in.qty[i];
            toInsert.add(new OrderLineReplica(
                    in.o_id, in.d_id, in.w_id, olNumber,
                    in.itemsIds[i], in.supWares[i],
                    qty, amount, in.ol_dist_info[i]
            ));
            addRevenue(amount);
            addChq1(olNumber, amount, qty);
        }
        this.orderLineReplicaRepository.insertAll(toInsert);
    }
}