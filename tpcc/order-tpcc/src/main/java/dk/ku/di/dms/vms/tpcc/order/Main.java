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
 * Port of the TPC-C order-related code as a virtual micro service.
 *
 * BUFFER_HEADROOM rationale:
 * The order_line table is the most contended hash buffer in the system —
 * high insert rate from new_order, high delete rate from FIFO eviction,
 * and constant scans from OLAP CHQ6. Open-addressing buffers under sustained
 * delete-heavy churn accumulate tombstones, and probes still walk past them.
 * At num_ware=1 with default sizing we observed SEVERE "Cannot find an
 * empty entry" entries near the end of HATtrick runs even at 30% load
 * factor — clustering, not absolute load, was the problem.
 *
 * With 4× headroom:
 *   num_ware=2 → numOrderLine = 260_000 * 10 * 4 = 10_400_000
 *     → buffer = next_pow2(10_400_000) = 16_777_216 (16M)
 *     → peak active rows ~2.5M → load factor ~15%
 *   num_ware=4 → numOrderLine = 320_000 * 10 * 4 = 12_800_000
 *     → buffer = 16M → peak active ~5M → load factor ~30%
 *
 * Memory cost: at num_ware=4 the order_line buffer is ~1.7 GB direct memory.
 * Comfortable within -XX:MaxDirectMemorySize=64G.
 *
 * The fixed +200_000 in numOrders is HATtrick run headroom (20k tx/s burst
 * × 10 s) — unchanged from before.
 */
public final class Main {

    /**
     * Multiplier applied to expected row count when sizing the hash buffer.
     * Aims for ~30% peak load factor on order_line under sustained HATtrick
     * churn, well below the ~50% degradation threshold for open addressing.
     */
    private static final int BUFFER_HEADROOM = 4;

    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);

        // num orders fixed = 30k * num_ware
        int numOrders = num_ware * 30_000;
        // based on 20k tx/s and 10s run (HATtrick burst headroom)
        numOrders += (20_000 * 10);
        int numOrderLine = numOrders * 10;

        prop.setProperty("max_records.orders",
                String.valueOf(numOrders * BUFFER_HEADROOM));
        prop.setProperty("max_records.new_orders",
                String.valueOf(numOrders * BUFFER_HEADROOM));
        prop.setProperty("max_records.order_line",
                String.valueOf(numOrderLine * BUFFER_HEADROOM));

        // history is capped at ~100K by the sliding-window FIFO eviction
        // in OrderService.processPayment, so 500K is already generously
        // over-provisioned (load factor ~19% even pre-eviction).
        prop.setProperty("max_records.history", "500000");

        prop.setProperty("table.orders.chaining", "false");
        prop.setProperty("table.new_orders.chaining", "false");
        prop.setProperty("table.order_line.chaining", "false");
        prop.setProperty("table.history.chaining", "false");
        prop.setProperty("checkpointing", "true");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options, (x,y) -> new OrderHttpHandler(x,
                        (IOrderRepository) y.apply("orders"),
                        (INewOrderRepository) y.apply("new_orders"),
                        (IOrderLineRepository) y.apply("order_line"),
                        (IHistoryRepository) y.apply("history")
                )
        );
    }
}