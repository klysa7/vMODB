package dk.ku.di.dms.vms.tpcc.inventory;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.inventory.infra.InventoryHttpHandler;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IItemRepository;
import dk.ku.di.dms.vms.tpcc.inventory.repositories.IStockRepository;

/**
 * Port of the TPC-C inventory-related code as a virtual microservice
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8002, new String[]{
                        "dk.ku.di.dms.vms.tpcc.inventory",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        VmsApplication vmsApplication = VmsApplication.build(options, (x,y) -> new InventoryHttpHandler(x,
                (IItemRepository) y.apply("item"),
                (IStockRepository) y.apply("stock")
        ));

        System.out.println(vmsApplication.internalChannels());
        return vmsApplication;
    }
}
