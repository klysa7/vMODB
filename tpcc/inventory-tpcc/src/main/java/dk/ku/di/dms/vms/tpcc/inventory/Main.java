package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.inventory.infra.InventoryHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

import java.util.Properties;

/**
 * Port of the TPC-C inventory-related code as a virtual micro service.
 *
 * BUFFER_HEADROOM rationale:
 * stock has moderate churn under TPC-C payment transactions (s_quantity
 * updates). item is fixed at 100K rows by TPC-C spec — read-only, but
 * still benefits from headroom to avoid clustering at population time.
 * Same 4× multiplier as warehouse/order Mains for consistency.
 *
 * With 4× headroom:
 *   item: 400_000 → buffer 524_288 → load factor 19%
 *   stock at num_ware=2: 800_000 → buffer 1_048_576 → load factor 19%
 *   stock at num_ware=4: 1_600_000 → buffer 2_097_152 → load factor 19%
 */
public final class Main {

    /**
     * Multiplier applied to expected row count when sizing the hash buffer.
     * See warehouse/order Main.java for the broader rationale.
     */
    private static final int BUFFER_HEADROOM = 4;

    public static void main(String[] args) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        int num_ware = Integer.parseInt(prop.getProperty("num_ware"));

        // item: fixed 100K rows (TPC-C spec, independent of num_ware)
        prop.setProperty("max_records.item",
                String.valueOf(100_000 * BUFFER_HEADROOM));

        // stock: 100K per warehouse (TPC-C spec)
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