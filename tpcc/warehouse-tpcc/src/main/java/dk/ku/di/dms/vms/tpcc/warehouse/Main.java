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
 * TPC-C warehouse tables (warehouse, district, customer) as a virtual microservice.
 *
 * vMODB's UniqueHashBufferIndex uses linear-probing open addressing, which degrades sharply
 * past ~50% load — worse once deletes leave tombstones. Hash buffers are sized at
 * BUFFER_HEADROOM (4×) the expected row count to keep peak load near 25%, which avoids the
 * "Cannot find an empty entry" failures seen at num_ware=2 with no headroom. customer scales
 * at 30K rows/warehouse; raise the multiplier if SEVERE probe-limit errors appear under
 * sustained high-churn runs.
 */
public final class Main {
    private static final int BUFFER_HEADROOM = 1;

    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        Properties prop = ConfigUtils.loadProperties();
        String numWareStr = prop.getProperty("num_ware");
        int num_ware = Integer.parseInt(numWareStr);

        prop.setProperty("max_records.warehouse",
                String.valueOf(num_ware * BUFFER_HEADROOM));

        int numDistrict = num_ware * 10 * BUFFER_HEADROOM;
        prop.setProperty("max_records.district", String.valueOf(numDistrict));
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