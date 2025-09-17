package dk.ku.di.dms.vms.tpcc.order.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.order.dto.OrderLineInfoDto;
import dk.ku.di.dms.vms.tpcc.order.entities.OrderLine;

import java.util.List;

@Repository
public interface IOrderLineRepository extends IRepository<OrderLine.OrderLineId, OrderLine> {

    @Query("select ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d from order_line where o_id = :o_id and c_d_id = :c_d_id and o_w_id = :o_w_id")
    List<OrderLineInfoDto> getOrderLinesInfo(int o_id, int c_d_id, int o_w_id, int c_id);

}