package dk.ku.di.dms.vms.tpcc.warehouse;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.warehouse.infra.WarehouseHttpHandler;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.ICustomerRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IDistrictRepository;
import dk.ku.di.dms.vms.tpcc.warehouse.repositories.IWarehouseRepository;

/**
 * Port of the TPC-C warehouse-related code as a virtual microservice
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8001, new String[]{
                        "dk.ku.di.dms.vms.tpcc.warehouse",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        VmsApplication vmsApplication = VmsApplication.build(options,
                (x,y) -> new WarehouseHttpHandler(x,
                        (IWarehouseRepository) y.apply("warehouse"),
                        (IDistrictRepository) y.apply("district"),
                        (ICustomerRepository) y.apply("customer")
                ));
        System.out.println(vmsApplication.internalChannels());
        return vmsApplication;
    }

}