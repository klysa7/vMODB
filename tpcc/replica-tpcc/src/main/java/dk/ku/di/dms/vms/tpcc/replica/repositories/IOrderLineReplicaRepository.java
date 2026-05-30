package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;


@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {
}