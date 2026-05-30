package dk.ku.di.dms.vms.calcite;

import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;

import java.io.IOException;

public final class Main {

    public static void main(String[] args) throws IOException {
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