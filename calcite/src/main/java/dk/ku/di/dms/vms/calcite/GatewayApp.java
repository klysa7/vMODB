package dk.ku.di.dms.vms.calcite;


import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.calcite.http.GatewayHttpHandler;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Function;

public final class GatewayApp {

    private final Function<String, CoordinatorClient> coordinatorClientFactory;

    public GatewayApp(Function<String, CoordinatorClient> coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
    }

    public HttpServer start(String bindHost, int port, String coordinatorUrl) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);

        CoordinatorClient coordinatorClient = coordinatorClientFactory.apply(coordinatorUrl);
        OlapGatewayService service = new OlapGatewayService(coordinatorClient);

        server.createContext("/", new GatewayHttpHandler(service));
        server.setExecutor(null);
        server.start();

        return server;
    }
}