package dk.ku.di.dms.vms.calcite;

import dk.ku.di.dms.vms.calcite.config.GatewayConfig;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;

import java.io.IOException;

public final class Main {

    public static void main(String[] args) throws IOException {
        // A5 FIX: CalciteVmsNode.start() removed entirely.
        //
        // CalciteVmsNode started a VmsEventHandler on port 9095 with a completely
        // empty TransactionManager (no tables, no data). No query ever routed to
        // port 9095. The Bridge class exposed an input queue that nothing read.
        // It was dead infrastructure that consumed a port, a thread, and memory
        // for no reason.
        //
        // Fix: just delete the call. The gateway operates purely via GatewayApp.
        // Also delete CalciteVmsNode.java from the source tree — it is now unused.

        GatewayConfig config = GatewayConfig.fromEnv();
        GatewayApp gatewayApp = new GatewayApp(CoordinatorClient::new);

        gatewayApp.start(
                config.getBindHost(),
                config.getPort(),
                config.getCoordinatorHttpUrl(),
                config.getCoordinatorSseUrl()
        );
    }
}