package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.inventory.infra.InventoryHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.Properties;

/**
 * TPC-C inventory tables (item, stock) as a virtual micro service.
 * Hash buffers are sized at BUFFER_HEADROOM (4×) the expected row count to keep
 * the load factor low (~19%) and avoid clustering at population time. item is
 * fixed at 100K rows by the TPC-C spec; stock is 100K per warehouse, so it scales
 * with num_ware. Same multiplier as the warehouse and order Mains.
 */
public final class Main {

    private static final int BUFFER_HEADROOM = 1;

    public static void main(String[] args) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        int num_ware = Integer.parseInt(prop.getProperty("num_ware"));

        // fixed
        prop.setProperty("max_records.item",
                String.valueOf(100_000 * BUFFER_HEADROOM));
        // variable
        int numStockItems = num_ware * 100_000 * BUFFER_HEADROOM;
        prop.setProperty("max_records.stock", String.valueOf(numStockItems));

        prop.setProperty("table.stock.chaining", "false");
        prop.setProperty("checkpointing", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8002, new String[]{
                        "dk.ku.di.dms.vms.tpcc.inventory",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x,y) -> new InventoryHttpHandler(x,
                (IItemRepository) y.apply("item"),
                (IStockRepository) y.apply("stock")
        ));
    }
}