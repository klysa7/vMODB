package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;

import java.util.List;

/**
 * Mirrors the Seller pattern exactly:
 *   Seller:  @Query("select * from order_entries where seller_id = :sellerId")
 *   Replica: @Query("select * from order_line")
 *
 * The HTTP handler calls beginTransaction(lastTid, 0, lastTid, true) before
 * invoking the query, giving the same MVCC snapshot guarantee as the live
 * order VMS's chq6 scan.
 *
 * We use select * + aggregate in Java instead of SELECT SUM(ol_amount)
 * because vMODB's aggregate queries crash on composite PK tables with
 * "Index 7 out of bounds for length 4".
 */
@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {

    @Query("select * from order_line where ol_quantity >= 1 and ol_quantity <= 100000")
    List<OrderLineReplica> getOrderLinesForChq6();
}