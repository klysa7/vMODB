package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;

import java.util.List;

/**
 * Mirrors the Seller pattern exactly:
 *
 *   Seller:
 *     @VmsIndex(name="seller_idx") on seller_id
 *     @Query("select * from order_entries where seller_id = :sellerId")
 *
 *   Replica:
 *     @VmsIndex(name="w_idx") on ol_w_id  (in OrderLineReplica entity)
 *     @Query("select * from order_line where ol_w_id = :wId")
 *
 * With num_ware=1, querying wId=1 returns ALL rows — semantically identical
 * to a full table scan, but using the index code path that doesn't crash.
 *
 * The HTTP handler calls beginTransaction(lastTid, 0, lastTid, true) before
 * invoking this method, giving MVCC snapshot consistency exactly like Seller.
 */
@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {

    @Query("select * from order_line where ol_w_id = :wId")
    List<OrderLineReplica> getOrderLinesByWarehouse(int wId);
}