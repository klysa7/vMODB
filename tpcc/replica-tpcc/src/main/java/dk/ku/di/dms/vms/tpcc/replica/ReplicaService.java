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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

/**
 * Replica VMS service — terminal node in the new_order DAG.
 *
 * Responsibilities:
 *   1. Insert order_line rows into the replica table when new-order-out
 *      events arrive.
 *   2. Evict old order_line rows so the table stays at a stable size.
 *
 * Eviction strategy
 * ─────────────────
 * Mirrors the live OrderService eviction pattern exactly: per-(w_id, d_id)
 * AtomicInteger counters track the current order-id frontier, and once the
 * frontier exceeds EVICTION_SAFE_THRESHOLD, every new insert evicts the
 * oldest order's lines from that same district. Net-zero growth: ~10 rows
 * inserted, ~10 rows deleted per new_order transaction.
 *
 *   Live OrderService:    counter = next o_id to insert; floor = oldest
 *                         o_id still present. Both per-(w_id, d_id).
 *   Replica (this file):  same scheme, isolated state on the replica side.
 *
 * Why O(1) and not query-based:
 *   The deterministic TPC-C order numbering (o_id increments densely
 *   within each district) means we don't need to ask the table what the
 *   oldest order is — we know it's exactly evictionFloor[(w,d)]. We
 *   construct that order's primary key directly and call deleteByKey.
 *   No scan, no @Query, no PK-prefix planner. Just 10 composite-PK
 *   deletes per new order.
 *
 * Steady-state target:
 *   EVICTION_SAFE_THRESHOLD = 3,000 → ~300K rows for num_ware=1.
 *   Matches the populate baseline.
 *
 * MVCC note:
 *   The deletes happen inside the same @Transactional(W) handler as the
 *   inserts, so they're visible together at commit. OLAP scans through
 *   /chq6 and /chq1 open a separate read-only snapshot at lastTidFinished()
 *   and never see partial state.
 */
@Microservice("replica")
public final class ReplicaService {

    private static final System.Logger LOGGER = System.getLogger(ReplicaService.class.getName());

    /**
     * Maximum number of orders per (w_id, d_id) before eviction kicks in.
     * Set to 3,000 to match the populate baseline (1 ware × 10 districts ×
     * 3,000 orders × 10 lines = 300,000 rows total).
     */
    public static final int EVICTION_SAFE_THRESHOLD = 3_000;

    private final IOrderLineReplicaRepository orderLineReplicaRepository;

    /**
     * Per-(w_id, d_id) "next order to evict" frontier. Starts at 1 (the
     * lowest o_id from populate); advances by one each time eviction fires.
     * Key encodes (w_id, d_id) as (w_id * 16 + d_id) — d_id ≤ 10, so 16
     * is a safe shift.
     */
    private final ConcurrentHashMap<Integer, AtomicInteger> evictionFloor = new ConcurrentHashMap<>();

    public ReplicaService(IOrderLineReplicaRepository orderLineReplicaRepository) {
        this.orderLineReplicaRepository = orderLineReplicaRepository;
    }

    @Inbound(values = "new-order-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderOut in) {
        // ── 1. Insert the new order's lines ──────────────────────────────
        List<OrderLineReplica> toInsert = new ArrayList<>(in.itemsIds.length);
        for (int i = 0; i < in.itemsIds.length; i++) {
            toInsert.add(new OrderLineReplica(
                    in.o_id, in.d_id, in.w_id, i + 1,
                    in.itemsIds[i], in.supWares[i],
                    in.qty[i], in.ol_amounts[i], in.ol_dist_info[i]
            ));
        }
        this.orderLineReplicaRepository.insertAll(toInsert);

        // ── 2. Evict if we've crossed the threshold ──────────────────────
        //
        // The condition is "the order we just inserted is far enough ahead
        // of our floor that we should retire the floor's row set." On
        // populate, every district starts with 3,000 orders (1..3000), so
        // when OLTP inserts o_id=3001 into a district, we immediately
        // evict o_id=1 from that same district to maintain ~3,000 in
        // flight. Same for 3002 vs 2, etc.
        //
        // We use compute-if-absent so that the first time we see a
        // (w_id, d_id) we anchor the floor at o_id=1 (consistent with
        // populate). For runs without populate the floor still starts at
        // 1 and advances naturally — no rows exist to evict until we've
        // crossed the threshold for real.
        if (in.o_id > EVICTION_SAFE_THRESHOLD) {
            int districtKey = (in.w_id << 4) | in.d_id;
            AtomicInteger floor = this.evictionFloor.computeIfAbsent(
                    districtKey, _ -> new AtomicInteger(1));

            // Try to claim a single eviction slot. Atomic inc-and-get
            // ensures two concurrent new-orders for the same district
            // each evict a *different* old order — no double-delete.
            int oldOid = floor.getAndIncrement();

            // Defensive: only evict if oldOid is at least one full
            // threshold-window behind the inserter. Skips the very first
            // few inserts after threshold-crossing where the floor hasn't
            // caught up yet.
            if (in.o_id - oldOid >= EVICTION_SAFE_THRESHOLD) {
                evictOrder(in.w_id, in.d_id, oldOid);
            } else {
                // Roll back the increment — this slot wasn't ours to claim.
                floor.decrementAndGet();
            }
        }
    }

    /**
     * Delete all order_line rows for a single (w_id, d_id, o_id) tuple.
     * TPC-C orders have between 5 and 15 lines (ol_number 1..ol_cnt). We
     * don't know ol_cnt for the evicted order, but issuing deleteByKey
     * for ol_number 1..15 is harmless: the framework silently ignores
     * deletes for non-existent keys.
     */
    private void evictOrder(int w_id, int d_id, int o_id) {
        for (int ol = 1; ol <= 15; ol++) {
            try {
                this.orderLineReplicaRepository.deleteByKey(
                        new OrderLineReplica.OrderLineReplicaId(o_id, d_id, w_id, ol));
            } catch (Exception e) {
                // Non-existent ol_number (>ol_cnt) → no-op. Real failures
                // on the early ol_numbers would indicate a bigger problem
                // and would be caught by the next insert's FK check.
                if (ol <= 5) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Eviction failure for (" + w_id + "," + d_id + "," + o_id
                                    + ",ol=" + ol + "): " + e.getMessage());
                }
            }
        }
    }
}