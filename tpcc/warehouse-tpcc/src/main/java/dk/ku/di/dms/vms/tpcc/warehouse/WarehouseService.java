package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.api.annotations.*;
import dk.ku.di.dms.vms.tpcc.common.events.*;
import dk.ku.di.dms.vms.tpcc.warehouse.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.District;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.Date;
import java.util.List;

import static dk.ku.di.dms.vms.modb.api.enums.TransactionTypeEnum.RW;

@Microservice("warehouse")
public final class WarehouseService {

    private final IWarehouseRepository warehouseRepository;
    private final IDistrictRepository  districtRepository;
    private final ICustomerRepository  customerRepository;

    public WarehouseService(IWarehouseRepository warehouseRepository,
                            IDistrictRepository  districtRepository,
                            ICustomerRepository  customerRepository) {
        this.warehouseRepository = warehouseRepository;
        this.districtRepository  = districtRepository;
        this.customerRepository  = customerRepository;
    }

    @Inbound(values = "new-order-ware-in")
    @Outbound("new-order-ware-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = NewOrderWareIn.class, method = "getId")
    public NewOrderWareOut processNewOrder(NewOrderWareIn in) {

        float w_tax = this.warehouseRepository.getWarehouseTax(in.w_id);

        DistrictInfoDTO distInfo = this.districtRepository.getNextOidAndTax(in.w_id, in.d_id);
        District district = this.districtRepository.lookupByKey(
                new District.DistrictId(in.d_id, in.w_id));
        district.d_next_o_id++;
        this.districtRepository.update(district);

        float c_discount = this.customerRepository.getDiscount(in.c_id, in.d_id, in.w_id);

        return new NewOrderWareOut(
                in.w_id,
                in.d_id,
                in.c_id,
                in.itemsIds,
                in.supWares,
                in.qty,
                in.allLocal,
                w_tax,
                distInfo.d_next_o_id(),
                distInfo.d_tax(),
                c_discount,
                in.client_id
        );
    }

    @Inbound(values = "payment-in")
    @Outbound("payment-out")
    @Transactional(type = RW)
    @PartitionBy(clazz = PaymentIn.class, method = "getId")
    public PaymentOut processPayment(PaymentIn in) {

        Warehouse warehouse = this.warehouseRepository.lookupByKey(in.w_id);
        warehouse.w_ytd += in.amount;
        this.warehouseRepository.update(warehouse);

        District district = this.districtRepository.lookupByKey(
                new District.DistrictId(in.d_id, in.w_id));
        district.d_ytd += in.amount;
        this.districtRepository.update(district);

        Customer customer;
        if (in.by_name) {
            List<Customer> customers = this.customerRepository.getCustomerByLastName(
                    in.c_d_id, in.c_w_id, in.c_last);
            if (customers == null || customers.isEmpty()) {
                // Index not yet built or no match — fall back to lookup by ID
                customer = this.customerRepository.lookupByKey(
                        new Customer.CustomerId(in.c_id, in.c_d_id, in.c_w_id));
            } else {
                customer = customers.get((customers.size() - 1) / 2);
            }
        } else {
            customer = this.customerRepository.lookupByKey(
                    new Customer.CustomerId(in.c_id, in.c_d_id, in.c_w_id));
        }

        customer.c_balance     -= in.amount;
        customer.c_ytd_payment += in.amount;
        customer.c_payment_cnt++;

        String data;
        if (customer.c_credit.equals("BC")) {
            String newInfo = in.c_id + " " + in.c_d_id + " " + in.c_w_id
                    + " " + in.d_id + " " + in.w_id + " " + in.amount;
            String combined = newInfo + "|" + customer.c_data;
            data = combined.substring(0, Math.min(combined.length(), 500));
            customer.c_data = data;
        } else {
            data = customer.c_first + " " + customer.c_middle + " " + customer.c_last;
        }
        this.customerRepository.update(customer);

        return new PaymentOut(
                in.w_id,
                in.d_id,
                customer.c_id,
                in.c_w_id,
                in.c_d_id,
                in.amount,
                data,
                in.client_id
        );
    }

    @Inbound(values = "order-status-in")
    @Outbound("order-status-out")
    @Transactional(type = RW)
    public OrderStatusOut processOrderStatus(OrderStatusIn in) {

        Customer customer;
        if (in.by_name) {
            List<Customer> customers = this.customerRepository.getCustomerByLastName(
                    in.d_id, in.w_id, in.c_last);
            if (customers == null || customers.isEmpty()) {
                customer = this.customerRepository.lookupByKey(
                        new Customer.CustomerId(in.c_id, in.d_id, in.w_id));
            } else {
                customer = customers.get((customers.size() - 1) / 2);
            }
        } else {
            customer = this.customerRepository.lookupByKey(
                    new Customer.CustomerId(in.c_id, in.d_id, in.w_id));
        }

        return new OrderStatusOut(in.w_id, in.d_id, customer.c_id);
    }
}