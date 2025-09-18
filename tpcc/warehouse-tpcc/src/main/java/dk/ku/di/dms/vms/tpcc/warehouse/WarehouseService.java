package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.modb.api.query.builder.QueryBuilderFactory;
import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareOut;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusOut;
import dk.ku.di.dms.vms.tpcc.warehouse.dto.CustomerInfoDTO;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.R;
import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;
import static java.lang.System.Logger.Level.DEBUG;

@Microservice("warehouse")
public final class WarehouseService {

    private static final System.Logger LOGGER = System.getLogger(WarehouseService.class.getName());

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository districtRepository;
    private final ICustomerRepository customerRepository;

    public WarehouseService(IWarehouseRepository warehouseRepository, IDistrictRepository districtRepository, ICustomerRepository customerRepository){
        this.warehouseRepository = warehouseRepository;
        this.districtRepository = districtRepository;
        this.customerRepository = customerRepository;
    }

    @VmsPreparedStatement("orderStatusCustomerQuery")
    public static final SelectStatement ORDER_STATUS_BASE_QUERY = QueryBuilderFactory.select()
            .project("c_balance").project("c_first").project("c_middle").project("c_last")
            .from("customer")
            .where("c_w_id", ExpressionTypeEnum.EQUALS, ":c_w_id")
            .and("c_d_id", ExpressionTypeEnum.EQUALS, ":c_d_id")
            .and("c_last", ExpressionTypeEnum.EQUALS, ":c_last")
            .orderBy( "c_first" ).build();

    @Inbound(values = "order-status-in")
    @Outbound("order-status-out")
    @Transactional(type = R)
    @PartitionBy(clazz = OrderStatusIn.class, method = "getId")
    public OrderStatusOut processOrderStatus(OrderStatusIn in) {
        if(in.by_name){
            List<CustomerInfoDTO> customers = this.issueOrderStatusQuery(in);
            LOGGER.log(DEBUG, customers);
        } else {
            Customer customer = this.customerRepository.lookupByKey(new Customer.CustomerId(in.c_id, in.d_id, in.w_id));
            LOGGER.log(DEBUG, customer);
        }
        return new OrderStatusOut(in.w_id, in.d_id, in.c_id);
    }

    public List<CustomerInfoDTO> issueOrderStatusQuery(OrderStatusIn in) {
        return this.customerRepository
                .fetchMany(ORDER_STATUS_BASE_QUERY.setParam( in.w_id, in.d_id, in.c_last ), CustomerInfoDTO.class);
    }

    @Inbound(values = "new-order-ware-in")
    @Outbound("new-order-ware-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareIn.class, method = "getId")
    public NewOrderWareOut processNewOrder(NewOrderWareIn in) {

        District district = this.districtRepository.lookupByKey(new District.DistrictId(in.d_id, in.w_id));
        float w_tax = this.warehouseRepository.getWarehouseTax(in.w_id);

        district.d_next_o_id++;
        this.districtRepository.update(district);

        float c_discount = this.customerRepository.getDiscount(in.w_id, in.d_id, in.c_id);

        return new NewOrderWareOut(
                in.w_id,
                in.d_id,
                in.c_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                in.allLocal,
                w_tax,
                district.d_next_o_id,
                district.d_tax,
                c_discount
        );
    }

}