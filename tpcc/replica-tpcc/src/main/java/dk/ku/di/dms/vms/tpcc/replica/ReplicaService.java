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
import static java.lang.System.Logger.Level.INFO;

/**
 * Replica VMS — terminal node in the new_order DAG.
 *
 * CHQ6 and CHQ1 are served via AtomicLong counters (O(1), no scan needed).
 * The replica table grows throughout the experiment — this is acceptable
 * because the OLAP queries never scan it (counters only).
 *
 * No eviction: OrderLineReplica has no @VmsForeignKey so delete would not
 * crash, but since CHQ6/CHQ1 use counters the table size is irrelevant.
 */
@Microservice("replica")
public final class ReplicaService {

    private static final System.Logger LOGGER = System.getLogger(ReplicaService.class.getName());

    // ── CHQ6: total revenue ───────────────────────────────────────────────────
    static final AtomicLong REVENUE_BITS = new AtomicLong(
            Float.floatToRawIntBits(0.0f) & 0xFFFFFFFFL);

    // ── CHQ1: per-ol_number aggregates ───────────────────────────────────────
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

    // ── Size tracking ─────────────────────────────────────────────────────────
    private static final AtomicLong TOTAL_INSERTS = new AtomicLong(0);
    private static volatile long lastLogMs = 0;
    private static final long LOG_INTERVAL_MS = 10_000;

    // ── CAS helpers ───────────────────────────────────────────────────────────

    static void addRevenue(float delta) { addFloatAtomic(REVENUE_BITS, delta); }

    static float getRevenue() {
        return Float.intBitsToFloat((int) REVENUE_BITS.get());
    }

    static void addChq1(int olNumber, float amount, int quantity) {
        int idx = olNumber - 1;
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
            float nextF = Float.intBitsToFloat((int) prev) + delta;
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

        // No eviction needed — CHQ6/CHQ1 served from AtomicLong counters,
        // not from table scans. Table growth does not affect query performance.

        long inserted = TOTAL_INSERTS.addAndGet(in.itemsIds.length);

        long nowMs = System.currentTimeMillis();
        if (nowMs - lastLogMs >= LOG_INTERVAL_MS) {
            lastLogMs = nowMs;
            long estimated = 300_000L + inserted; // populate seeded 300K
            LOGGER.log(INFO,
                    "[replica order_line size] estimated={0} rows " +
                            "(populate=300000 + tx_inserted={1}). " +
                            "CHQ6 revenue counter={2}",
                    estimated, inserted, getRevenue());
        }
    }
}