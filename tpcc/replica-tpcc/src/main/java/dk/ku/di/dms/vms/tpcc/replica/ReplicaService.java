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
 * Revenue is tracked incrementally via a thread-safe AtomicLong (stored
 * as float bits to avoid floating-point race conditions). This avoids
 * the need for a full table scan on every OLAP query, and sidesteps
 * vMODB's "Index 7 out of bounds for length 4" bug with select * on
 * composite PK tables.
 *
 * The HTTP handler in Main reads REVENUE_BITS directly — O(1) per query,
 * no MVCC snapshot needed.
 */
@Microservice("replica")
public final class ReplicaService {

    /**
     * Running sum of ol_amount across all committed new_order transactions.
     * Stored as raw float bits in a long for atomic CAS updates.
     *
     * At populate time (300K rows × 10.0f each) this starts at 3,000,000.0.
     * Each new_order adds ~10 lines × ol_amount each.
     *
     * Exposed as a package-visible static so Main's HTTP handler can read it
     * without going through the VMS transaction manager.
     */
    static final AtomicLong REVENUE_BITS = new AtomicLong(
            Float.floatToRawIntBits(0.0f));

    /**
     * Adds a delta to the running revenue total using a CAS loop.
     * Thread-safe for @Parallel transactions.
     */
    static void addRevenue(float delta) {
        long prev, next;
        do {
            prev = REVENUE_BITS.get();
            float prevFloat = Float.intBitsToFloat((int) prev);
            float nextFloat = prevFloat + delta;
            next = Float.floatToRawIntBits(nextFloat) & 0xFFFFFFFFL;
        } while (!REVENUE_BITS.compareAndSet(prev, next));
    }

    static float getRevenue() {
        return Float.intBitsToFloat((int) REVENUE_BITS.get());
    }

    private final IOrderLineReplicaRepository orderLineReplicaRepository;

    public ReplicaService(IOrderLineReplicaRepository orderLineReplicaRepository) {
        this.orderLineReplicaRepository = orderLineReplicaRepository;
    }

    @Inbound(values = "new-order-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderOut in) {
        List<OrderLineReplica> toInsert = new ArrayList<>(in.itemsIds.length);
        float batchRevenue = 0.0f;
        for (int i = 0; i < in.itemsIds.length; i++) {
            toInsert.add(new OrderLineReplica(
                    in.o_id, in.d_id, in.w_id, i + 1,
                    in.itemsIds[i], in.supWares[i],
                    in.qty[i], in.ol_amounts[i], in.ol_dist_info[i]
            ));
            batchRevenue += in.ol_amounts[i];
        }
        this.orderLineReplicaRepository.insertAll(toInsert);
        addRevenue(batchRevenue);
    }
}