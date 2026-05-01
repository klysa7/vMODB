package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.entities.OrderLineReplica;

/**
 * Replica order_line repository — uses only the base IRepository methods.
 *
 * No @Query annotations. The two we need are inherited:
 *
 *   List<OrderLineReplica> getAll()              — scan-all, MVCC-aware
 *   void insertAll(List<OrderLineReplica>)       — bulk insert (used by populate)
 *
 * Why no @Query for our scans:
 *   The order_line composite PK is (ol_o_id, ol_d_id, ol_w_id, ol_number).
 *   vMODB's @Query planner only handles WHERE clauses that are a PK-prefix
 *   lookup or a single equality on the leading PK column. Anything else —
 *   filtering on a non-leading PK column like ol_w_id alone, on a non-PK
 *   column like ol_quantity, or no WHERE at all — fails at metadata-load
 *   time with
 *
 *     "Error on processing the query annotation: Index 7 out of bounds
 *      for length 4"
 *
 *   (the runtime form of which is "arraycopy: source index -1 out of
 *   bounds for int[0]"). It's a known framework limitation.
 *
 *   The Seller marketplace's @Query("select * from order_entries where
 *   seller_id = :sellerId") works because seller_id IS the leading PK of
 *   order_entries — a clean PK-prefix lookup. There is no analogous
 *   single-column equality on order_line that returns the whole table.
 *
 * IRepository.getAll() bypasses @Query entirely, so the planner never
 * runs. TransactionManager.getAll(Table) walks the primary key index
 * under the current transaction context — exactly the MVCC scan
 * semantics we want, identical in spirit to the seller pattern.
 */
@Repository
public interface IOrderLineReplicaRepository
        extends IRepository<OrderLineReplica.OrderLineReplicaId, OrderLineReplica> {
}