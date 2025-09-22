package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.facade.AbstractProxyRepository;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.warehouse.entities.Customer;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;

public class WarehouseTest {

    private static VmsApplication getVmsApplication() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build("localhost", 8001, new String[]{
                "dk.ku.di.dms.vms.tpcc.warehouse",
                "dk.ku.di.dms.vms.tpcc.common"
        });
        return VmsApplication.build(options);
    }

    static VmsApplication VMS;

    @BeforeClass
    public static void setUp() throws Exception {
        VMS = getVmsApplication();
        VMS.start();
    }

    @SuppressWarnings("unchecked")
    private static void insertCustomers(VmsApplication vms) {
        var customerRepository = (AbstractProxyRepository<Customer.CustomerId, Customer>) vms.getRepositoryProxy("customer");
        for(int i = 1; i <= 10; i++){
            Customer customer = new Customer(i, 1, 1,
                    "test"+i, "test", "test", "test",
                    "test", "test", "test", "test", "test",
                    new Date(), "test", 1,
                    1, 1, 1, 1, 1, "test" );
            VMS.getTransactionManager().beginTransaction(0, 0, 0, false);
            customerRepository.upsert(customer);
            Assert.assertTrue(customerRepository.exists(new Customer.CustomerId(i, 1 , 1)));
        }
    }

    private static void generateOrderStatus(VmsApplication vms, int tid, int previousTid) {
        OrderStatusIn orderStatusIn = new OrderStatusIn(1, 1, 1, "test", true);
        InboundEvent inboundEvent = new InboundEvent(tid, previousTid, 1,
                "order-status-in", OrderStatusIn.class, orderStatusIn);
        vms.internalChannels().transactionInputQueue().add(inboundEvent);
    }

    @Test
    public void testOrderStatusQueryByName() throws Exception {
        ICustomerRepository customerRepository = (ICustomerRepository) VMS.getRepositoryProxy("customer");
        insertCustomers(VMS);
        var txCtx = VMS.getTransactionManager().beginTransaction( 1, 0, 10, true );
        // get warehouse service
        WarehouseService warehouseService = VMS.getService("dk.ku.di.dms.vms.tpcc.warehouse.WarehouseService");
        OrderStatusIn orderStatusIn = new OrderStatusIn(1, 1, 1, "test", true);

        var customers = warehouseService.issueOrderStatusQuery(orderStatusIn);
        Assert.assertEquals(10, customers.size());

        var orderStatusOut = warehouseService.processOrderStatus(orderStatusIn);
        Assert.assertEquals(1, orderStatusOut.c_id);
    }

}
