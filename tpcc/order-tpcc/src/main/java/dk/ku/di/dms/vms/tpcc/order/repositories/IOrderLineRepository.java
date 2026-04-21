package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;

import java.util.List;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> {

    @Query("select ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount from order_line " +
            "where ol_o_id = :o_id and ol_d_id = :c_d_id and ol_w_id = :o_w_id")
    List<OrderLineInfoDto> getOrderLinesInfo(int o_id, int c_d_id, int o_w_id);

    @Query("select * from order_line where ol_o_id = :o_id and ol_d_id = :d_id and ol_w_id = :w_id")
    List<OrderLine> getAllByOrderId(int o_id, int d_id, int w_id);

    // ─────────────────────────────────────────────────────────────────────────
    // getOldestOrderLinesByDistrict was REMOVED.
    //
    // That query was
    //   "select * from order_line where ol_w_id = :w_id and ol_d_id = :d_id
    //    order by ol_o_id asc limit 15"
    //
    // The order_line PK is (ol_o_id, ol_d_id, ol_w_id, ol_number) — ol_o_id is
    // the PK prefix. Filtering on (ol_w_id, ol_d_id) without ol_o_id is NOT a
    // PK-prefix lookup and SimplePlanner falls through to FullScan. At the
    // post-populate baseline this table is hundreds of thousands of rows.
    // Running that query inside every processNewOrder transaction would scan
    // the whole table once per new_order — on the single-threaded order VMS
    // that collapses T-tps well below the 1800 baseline.
    //
    // Eviction is now done in OrderService via a per-(w_id, d_id) AtomicInteger
    // counter. Direct O(1) composite-PK deletes, no scan. See OrderService
    // EVICTION STRATEGY block.
    // ─────────────────────────────────────────────────────────────────────────
}