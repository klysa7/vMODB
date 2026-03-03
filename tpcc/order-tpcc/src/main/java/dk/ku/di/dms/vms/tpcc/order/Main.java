package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.order.infra.OrderHttpHandler;
import dk.ku.di.dms.vms.tpcc.order.repositories.IHistoryRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.INewOrderRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderLineRepository;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

import java.util.Properties;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);

        int numOrders = num_ware * 30_000;
        numOrders += (20_000 * 10);
        int numOrderLine = numOrders * 10;

        prop.setProperty("max_records.orders", String.valueOf(numOrders));
        prop.setProperty("max_records.new_orders", String.valueOf(numOrders));
        prop.setProperty("max_records.order_line", String.valueOf(numOrderLine));
        prop.setProperty("max_records.history", "500000");

        prop.setProperty("table.orders.chaining", "false");
        prop.setProperty("table.new_orders.chaining", "false");
        prop.setProperty("table.order_line.chaining", "false");
        prop.setProperty("table.history.chaining", "false");
        prop.setProperty("checkpointing", "true");

        prop.setProperty("max_records.customer", "1");
        prop.setProperty("max_records.warehouse", "1");
        prop.setProperty("max_records.district", "1");
        prop.setProperty("max_records.stock", "1");
        prop.setProperty("max_records.item", "1");

        LOGGER.log(INFO, ">>> [ORDER VMS] Registered global schemas for Warehouse and Inventory packages.");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.inventory",
                        "dk.ku.di.dms.vms.tpcc.common"
                });

        return VmsApplication.build(options, (x,y) -> {

            // ---> THE FIX: Force the TransactionManager to parse and register the remote schemas! <---
            y.apply("customer");
            y.apply("warehouse");
            y.apply("district");
            y.apply("stock");
            y.apply("item");

            return new OrderHttpHandler(x,
                    (IOrderRepository) y.apply("orders"),
                    (INewOrderRepository) y.apply("new_orders"),
                    (IOrderLineRepository) y.apply("order_line"),
                    (IHistoryRepository) y.apply("history")
            );
        });
    }
}