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

/**
 * Port of the TPC-C order-related code as a virtual micro service
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);

        // orders and new_orders have NO eviction — they grow monotonically.
        // At 12,000 tps × 50% new_order = ~6,000 new orders/sec.
        // A single full grid run (~15 min) produces ~5.4M new orders.
        // Allocate 3M per warehouse to survive multiple grid runs safely.
        int numOrders = num_ware * 3_000_000;

        // order_line is bounded by eviction (ol_cnt=3, stable at ~390K rows).
        // Buffer is sized for the full grid including growth phase.
        int numOrderLine = num_ware * 3_500_000;

        prop.setProperty("max_records.orders",     String.valueOf(numOrders));
        prop.setProperty("max_records.new_orders", String.valueOf(numOrders));
        prop.setProperty("max_records.order_line", String.valueOf(numOrderLine));
        prop.setProperty("max_records.history",    "500000");

        prop.setProperty("table.orders.chaining",     "false");
        prop.setProperty("table.new_orders.chaining", "false");
        prop.setProperty("table.order_line.chaining", "false");
        prop.setProperty("table.history.chaining",    "false");
        prop.setProperty("checkpointing", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x, y) -> new OrderHttpHandler(x,
                        (IOrderRepository)    y.apply("orders"),
                        (INewOrderRepository) y.apply("new_orders"),
                        (IOrderLineRepository) y.apply("order_line"),
                        (IHistoryRepository)  y.apply("history")
                )
        );
    }
}