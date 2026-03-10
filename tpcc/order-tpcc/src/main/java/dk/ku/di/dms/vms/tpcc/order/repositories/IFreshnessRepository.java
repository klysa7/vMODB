package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.entities.Freshness;

/**
 * Repository for the FRESHNESS_j table in the order VMS.
 * <p>
 * At startup, one row is inserted per T-client via
 * {@code insert(new Freshness(j, 0L))} from {@code SsbDataInit}.
 * <p>
 * At runtime, the order VMS HTTP handler exposes
 * {@code PATCH /freshness/{client_id}} which calls
 * {@code lookupByKey(clientId)} → increment → {@code update(f)}.
 */
@Repository
public interface IFreshnessRepository extends IRepository<Integer, Freshness> {
    // lookupByKey(int client_id) and update(Freshness) inherited from IRepository
}