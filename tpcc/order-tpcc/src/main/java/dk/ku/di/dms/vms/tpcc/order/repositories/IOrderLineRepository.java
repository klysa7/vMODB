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
    // EVICTION QUERY — Table Size Stabilization
    //
    // Returns the oldest order_line rows for a given warehouse and district,
    // ordered by ol_o_id ASC (lowest ol_o_id = oldest order).
    //
    // Used in processNewOrder() to delete one old order's worth of rows
    // every time a new order is inserted, keeping order_line at ~300K rows.
    //
    // LIMIT 15: ol_count is randomly chosen between 5 and 15 per TPC-C spec.
    // Using 15 ensures we retrieve at most one full order's lines in one query.
    // The delete loop then removes exactly those rows.
    //
    // WHY ol_o_id IS SAFE TO ORDER BY:
    //   ol_o_id is the first column of the composite PK (ol_o_id, ol_d_id,
    //   ol_w_id, ol_number). The primary index is ordered by the full PK,
    //   so scanning in ascending ol_o_id order is supported by the framework
    //   (same pattern as getLastOrderByCustomerId uses ORDER BY o_id DESC).
    //
    // NET EFFECT:
    //   +ol_cnt rows inserted (new order, 5-15 rows)
    //   -old_ol_cnt rows deleted (oldest order, 5-15 rows)
    //   Net ≈ 0 rows per transaction → table stays at ~300K
    //
    // PERFORMANCE IMPACT ON OLTP:
    //   Adds 1 SELECT + ~10 DELETE operations per new_order transaction.
    //   T-tps will decrease slightly (~5-10%). The professor confirmed
    //   increased latency is acceptable for stable scan performance.
    //
    // PERFORMANCE IMPACT ON OLAP:
    //   At τ=2, table stays at ~300K instead of growing to ~800K.
    //   Scan time stays constant throughout the 30s measurement window.
    //   Expected: A-qps at τ=2 converges toward τ=0 A-qps (~10 qps).
    // ─────────────────────────────────────────────────────────────────────────
    @Query("select * from order_line where ol_w_id = :w_id and ol_d_id = :d_id " +
            "order by ol_o_id asc limit 15")
    List<OrderLine> getOldestOrderLinesByDistrict(int w_id, int d_id);

}