package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.warehouse.infra.WarehouseHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

import java.util.Properties;

/**
 * Port of the TPC-C warehouse-related code as a virtual micro service.
 *
 * BUFFER_HEADROOM rationale:
 * vMODB's UniqueHashBufferIndex uses linear-probing open addressing.
 * Open addressing degrades sharply past ~50% load factor — even more so
 * when deletes leave tombstones that probes still must walk past. Without
 * headroom, num_ware=2 hit SEVERE during population:
 *
 *   max_records.customer = num_ware * 30_000 = 60_000 at num_ware=2
 *     → buffer slot count = next_pow2(60_000) = 65_536
 *     → load factor 91.5% → probe limit reached → SEVERE
 *
 * With 4× headroom:
 *   max_records.customer = 240_000 at num_ware=2
 *     → buffer = next_pow2(240_000) = 262_144
 *     → load factor 23%, comfortable for tombstone clustering
 *
 * 4× chosen empirically. Lower (2×) leaves load factor near 50% at peak
 * churn, which is unsafe with deletes. Higher (8×) wastes direct memory
 * with no measurable benefit for these table sizes.
 */
public final class Main {

    /**
     * Multiplier applied to expected row count when sizing the hash buffer.
     * Aims for ~25% peak load factor across all sized tables.
     * Increase if SEVERE "Cannot find an empty entry" appears under sustained
     * high-churn workloads (HATtrick at low sleep, large num_ware).
     */
    private static final int BUFFER_HEADROOM = 4;

    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);

        // warehouse: tiny table but give headroom for consistency
        // num_ware=2 → 8 slots; num_ware=4 → 16 slots
        prop.setProperty("max_records.warehouse",
                String.valueOf(num_ware * BUFFER_HEADROOM));

        // district: 10 districts per warehouse (TPC-C spec)
        // num_ware=2 → 80 slot target → buffer 128, load factor ~16%
        int numDistrict = num_ware * 10 * BUFFER_HEADROOM;
        prop.setProperty("max_records.district", String.valueOf(numDistrict));

        // customer: 30K customers per warehouse (TPC-C spec)
        // num_ware=2 → 240K slot target → buffer 262K, load factor ~23%
        // num_ware=4 → 480K slot target → buffer 524K, load factor ~23%
        int numCustomers = num_ware * 30_000 * BUFFER_HEADROOM;
        prop.setProperty("max_records.customer", String.valueOf(numCustomers));

        prop.setProperty("table.customer.chaining", "false");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8001, new String[]{
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        return VmsApplication.build(options,
                (x,y) -> new WarehouseHttpHandler(x,
                        (IWarehouseRepository) y.apply("warehouse"),
                        (IDistrictRepository) y.apply("district"),
                        (ICustomerRepository) y.apply("customer")
                ));
    }

}