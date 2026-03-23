package dk.ku.di.dms.vms.tpcc.replica;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderInvOut;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;
import dk.ku.di.dms.vms.tpcc.replica.repositories.IOrderLineReplicaRepository;

import java.util.ArrayList;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

/**
 * Replica VMS service — terminal node in the new_order transaction DAG.
 *
 * ┌─────────────────────────────────────────────────────────────────────┐
 * │  Transaction DAG for new_order (Experiment II):                     │
 * │                                                                     │
 * │   warehouse ──► inventory ──► order VMS   (terminal, port 8003)    │
 * │                            ╰──► replica   (terminal, port 8004)    │
 * │                                                                     │
 * │  Both order VMS and replica receive "new-order-inv-out" from        │
 * │  inventory. The coordinator commits only after BOTH send their      │
 * │  vote — so both are always in sync (freshness = 0).                 │
 * └─────────────────────────────────────────────────────────────────────┘
 *
 * What this solves for Experiment II:
 *   In Experiment I, OLAP queries scan the live order VMS index while
 *   OLTP transactions are inserting into it. The MVCC checkpoint lock
 *   creates contention — A-qps collapses from ~0.31 at τ=0 to ~0.03
 *   at τ=2.
 *
 *   The replica holds its own copy of order_line. OLAP queries routed
 *   to /olap/replica/chq6 scan the replica's index, which is never
 *   touched by the concurrent OLTP path. Expected: A-qps stays flat.
 *
 *   Trade-off: the coordinator now waits for one extra terminal vote per
 *   batch, adding a small latency overhead. T-tps may drop slightly vs
 *   Experiment I — that overhead IS the measurement for Experiment II.
 *
 * CHQ6_STMT: prepared statement for CH Q6 — SELECT SUM(ol_amount) FROM order_line.
 *   Registered at startup via @VmsPreparedStatement so Main's HTTP handler
 *   can call repository.fetchOne(CHQ6_STMT, ReplicaChq6Result.class).
 */
@Microservice("replica")
public final class ReplicaService {

    @VmsPreparedStatement("chq6")
    public static final SelectStatement CHQ6_STMT = QueryBuilderFactory.select()
            .sum("ol_amount")
            .from("order_line")
            .build();

    private final IOrderLineReplicaRepository orderLineReplicaRepository;

    public ReplicaService(IOrderLineReplicaRepository orderLineReplicaRepository) {
        this.orderLineReplicaRepository = orderLineReplicaRepository;
    }

    /**
     * Mirror of OrderService.processNewOrder() — inserts the same order_line
     * rows into the replica's own index.
     *
     * ol_amount formula matches OrderService exactly so SUM(ol_amount) results
     * are comparable between /olap/chq6 (live) and /olap/replica/chq6 (replica).
     *
     * @Parallel is safe: each transaction has a unique composite key
     * (ol_o_id, ol_d_id, ol_w_id, ol_number), so concurrent inserts
     * for different orders cannot conflict.
     */
    @Inbound(values = "new-order-inv-out")
    @Transactional(type = W)
    @Parallel
    public void processNewOrder(NewOrderInvOut in) {
        List<OrderLineReplica> toInsert = new ArrayList<>(in.itemsIds.length);
        for (int i = 0; i < in.itemsIds.length; i++) {
            float ol_amount = (float) (in.qty[i] * in.itemsIds[i]
                    * (1 + in.w_tax + in.d_tax) * (1 - in.c_discount));
            toInsert.add(new OrderLineReplica(
                    in.d_next_o_id,
                    in.d_id,
                    in.w_id,
                    i + 1,
                    in.itemsIds[i],
                    in.supWares[i],
                    null,           // ol_delivery_d — null at insert time
                    in.qty[i],
                    ol_amount,
                    in.ol_dist_info[i]
            ));
        }
        this.orderLineReplicaRepository.insertAll(toInsert);
    }
}