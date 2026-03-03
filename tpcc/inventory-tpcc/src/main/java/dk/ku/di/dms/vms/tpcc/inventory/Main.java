package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.inventory.infra.InventoryHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.Properties;
import static java.lang.System.Logger.Level.INFO;

public final class Main {

    private static final System.Logger LOGGER = System.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        int num_ware = Integer.parseInt(prop.getProperty("num_ware"));

        prop.setProperty("max_records.item", "100000");
        int numStockItems = num_ware * 100_000;
        prop.setProperty("max_records.stock", String.valueOf(numStockItems));
        prop.setProperty("table.stock.chaining", "false");
        prop.setProperty("checkpointing", "true");

        prop.setProperty("max_records.orders", "1");
        prop.setProperty("max_records.new_orders", "1");
        prop.setProperty("max_records.order_line", "1");
        prop.setProperty("max_records.history", "1");
        prop.setProperty("max_records.customer", "1");
        prop.setProperty("max_records.warehouse", "1");
        prop.setProperty("max_records.district", "1");

        LOGGER.log(INFO, ">>> [INVENTORY VMS] Registered global schemas for Order and Warehouse packages.");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8002, new String[]{
                        "dk.ku.di.dms.vms.tpcc.inventory",
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x,y) -> new InventoryHttpHandler(x,
                (IItemRepository) y.apply("item"),
                (IStockRepository) y.apply("stock")
        ));
    }
}