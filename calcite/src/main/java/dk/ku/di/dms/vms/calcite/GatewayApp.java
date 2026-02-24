package dk.ku.di.dms.vms.calcite;

import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient; // Import the TCP Client
import dk.ku.di.dms.vms.calcite.http.GatewayHttpHandler;
import dk.ku.di.dms.vms.calcite.monitor.SnapshotMonitor;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public final class GatewayApp {

    private final Function<String, CoordinatorClient> coordinatorClientFactory;
    private final AtomicLong globalSnapshotId = new AtomicLong(0);

    public GatewayApp(Function<String, CoordinatorClient> coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
    }

    public HttpServer start(String bindHost, int port, String httpUrl, String sseUrl) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);

        startSnapshotMonitor(sseUrl);
        CoordinatorClient coordinatorClient = coordinatorClientFactory.apply(httpUrl);

        VmsGatewayClient gatewayClient = new VmsGatewayClient();

        OlapGatewayService service = new OlapGatewayService(
                coordinatorClient,
                globalSnapshotId,
                gatewayClient
        );

        server.createContext("/", new GatewayHttpHandler(service));
        server.setExecutor(null);
        server.start();
        System.out.println("Gateway started on " + bindHost + ":" + port);
        System.out.println(" - Catalog Client: " + httpUrl);
        System.out.println(" - Snapshot Monitor: " + sseUrl);

        return server;
    }

    private void startSnapshotMonitor(String sseUrl) {
        try {
            URI uri = URI.create(sseUrl);
            SnapshotMonitor monitor = new SnapshotMonitor(uri.getHost(), uri.getPort(), this.globalSnapshotId);
            Thread monitorThread = new Thread(monitor);
            monitorThread.setName("Gateway-SnapshotMonitor");
            monitorThread.setDaemon(true);
            monitorThread.start();
        } catch (Exception e) {
            System.err.println("Failed to start SnapshotMonitor: " + e.getMessage());
        }
    }
}