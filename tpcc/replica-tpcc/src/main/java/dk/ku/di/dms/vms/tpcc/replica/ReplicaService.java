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

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.W;

/**
 * Replica VMS service — terminal node in the new_order DAG.
 *
 * Receives NewOrderOut events and inserts order_line rows into the
 * replica table. No AtomicLong counters — queries are answered by
 * the HTTP handler via vMODB's MVCC query engine, exactly like the
 * Seller VMS in the online marketplace.
 *
 * DAG position (same as Seller in marketplace):
 *   warehouse → inventory → order → REPLICA (terminal)
 *
 * Query answering (same pattern as Seller):
 *   Seller:  getOrderEntriesBySellerId() → sum in Java
 *   Replica: getOrderLinesForChq6()      → sum in Java
 */
@Microservice("replica")
public final class ReplicaService {

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
            toInsert.add(new OrderLineReplica(
                    in.o_id, in.d_id, in.w_id, i + 1,
                    in.itemsIds[i], in.supWares[i],
                    in.qty[i], in.ol_amounts[i], in.ol_dist_info[i]
            ));
        }
        this.orderLineReplicaRepository.insertAll(toInsert);
    }
}