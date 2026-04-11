package dk.ku.di.dms.vms.calcite;

import dk.ku.di.dms.vms.calcite.config.GatewayConfig;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;

import java.io.IOException;

public final class Main {

    public static void main(String[] args) throws IOException {
        // A5 FIX: CalciteVmsNode.start() removed entirely — dead infrastructure.

        // B1 FIX: load all config from application.properties (working dir or classpath).
        // Edit application.properties to change settings without recompiling.
        GatewayConfig config = GatewayConfig.load();

        GatewayApp gatewayApp = new GatewayApp(CoordinatorClient::new);

        gatewayApp.start(
                config.getBindHost(),
                config.getPort(),
                config.getCoordinatorHttpUrl(),
                config.getCoordinatorSseUrl(),
                config
        );
    }
}