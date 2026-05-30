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
 * Replica VMS service. A terminal node in the new_order DAG. On each new-order-out event,
 * inserts the order's order_line rows into the replica table, then evicts the oldest order's
 * lines from that district once the frontier passes EVICTION_SAFE_THRESHOLD (3,000), holding
 * the table at ~300K rows.  Deletes and inserts commit together; OLAP scans read
 * a separate read-only snapshot.
 */
@Microservice("replica")
public final class ReplicaService {

    private static final System.Logger LOGGER = System.getLogger(ReplicaService.class.getName());
    public static final int EVICTION_SAFE_THRESHOLD = 3_000;
    private final IOrderLineReplicaRepository orderLineReplicaRepository;
    private final ConcurrentHashMap<Integer, AtomicInteger> evictionFloor = new ConcurrentHashMap<>();

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

        if (in.o_id > EVICTION_SAFE_THRESHOLD) {
            int districtKey = (in.w_id << 4) | in.d_id;
            AtomicInteger floor = this.evictionFloor.computeIfAbsent(
                    districtKey, _ -> new AtomicInteger(1));

            int oldOid = floor.getAndIncrement();

            if (in.o_id - oldOid >= EVICTION_SAFE_THRESHOLD) {
                evictOrder(in.w_id, in.d_id, oldOid);
            } else {
                floor.decrementAndGet();
            }
        }
    }

    private void evictOrder(int w_id, int d_id, int o_id) {
        for (int ol = 1; ol <= 15; ol++) {
            try {
                this.orderLineReplicaRepository.deleteByKey(
                        new OrderLineReplica.OrderLineReplicaId(o_id, d_id, w_id, ol));
            } catch (Exception e) {
                if (ol <= 5) {
                    LOGGER.log(System.Logger.Level.WARNING,
                            "Eviction failure for (" + w_id + "," + d_id + "," + o_id
                                    + ",ol=" + ol + "): " + e.getMessage());
                }
            }
        }
    }
}