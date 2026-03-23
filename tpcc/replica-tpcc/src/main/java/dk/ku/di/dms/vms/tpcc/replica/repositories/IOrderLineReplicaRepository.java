package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;

/**
 * Repository for the replica's order_line table.
 *
 * insertAll() — inherited — used by ReplicaService to batch-insert
 *   order lines when a new-order-inv-out event arrives.
 *
 * fetchOne() — inherited — used by Main's HTTP handler to compute
 *   SUM(ol_amount) via the CHQ6_STMT prepared statement.
 */
@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {
    // All needed methods (insertAll, fetchOne) inherited from IRepository
}