package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;

/**
 * Repository for the replica's order_line table.
 *
 * insertAll() — inherited from IRepository, used by ReplicaService.
 *
 * No @Query methods — vMODB's select * fails on composite PK tables
 * ("Index 7 out of bounds for length 4"). Revenue is tracked
 * incrementally via ReplicaService.REVENUE_BITS instead.
 */
@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {
}