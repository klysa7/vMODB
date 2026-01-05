package dk.ku.di.dms.vms.tpcc.order;

import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.tpcc.order.infra.OrderHttpHandler;
import dk.ku.di.dms.vms.tpcc.order.repositories.IOrderRepository;

/**
 * Port of the TPC-C order-related code as a virtual microservice
 */
public final class Main {
    public static void main( String[] args ) throws Exception {
        build().start();
    }

    public static VmsApplication build() throws Exception {
        VmsApplicationOptions options = VmsApplicationOptions.build(
                "0.0.0.0",
                8003, new String[]{
                        "dk.ku.di.dms.vms.tpcc.order",
                        "dk.ku.di.dms.vms.tpcc.common"
                });
        VmsApplication vmsApplication = VmsApplication.build(options, (x,y) -> new OrderHttpHandler(x, (IOrderRepository) y.apply("order")));

        System.out.println(vmsApplication.internalChannels());
        return vmsApplication;
    }
}
