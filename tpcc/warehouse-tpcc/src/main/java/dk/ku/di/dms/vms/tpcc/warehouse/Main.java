package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.warehouse.infra.WarehouseHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

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

        prop.setProperty("max_records.warehouse", numWareStr);

        int numDistrict = num_ware * 10;
        prop.setProperty("max_records.district", String.valueOf(numDistrict));
        int numCustomers = num_ware * 30_000;
        prop.setProperty("max_records.customer", String.valueOf(numCustomers));
        prop.setProperty("table.customer.chaining", "false");

        prop.setProperty("max_records.orders", "1");
        prop.setProperty("max_records.new_orders", "1");
        prop.setProperty("max_records.order_line", "1");
        prop.setProperty("max_records.history", "1");
        prop.setProperty("max_records.stock", "1");
        prop.setProperty("max_records.item", "1");

        LOGGER.log(INFO, ">>> [WAREHOUSE VMS] Registered global schemas for Order and Inventory packages.");

        VmsApplicationOptions options = VmsApplicationOptions.build(
                prop,
                "0.0.0.0",
                8001, new String[]{
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.inventory",
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