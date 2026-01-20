package dk.ku.di.dms.vms.calcite;

import dk.ku.di.dms.vms.calcite.config.GatewayConfig;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;

import java.io.IOException;

public final class Main {

    public static void main(String[] args) throws IOException {

        GatewayConfig gatewayConfig = GatewayConfig.fromEnv();
        GatewayApp gatewayApp = new GatewayApp(CoordinatorClient::new);
        gatewayApp.start(gatewayConfig.getBindHost(), gatewayConfig.getPort(),
                gatewayConfig.getCoordinatorUrl());
    }
}