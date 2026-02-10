package dk.ku.di.dms.vms.calcite;

import com.sun.net.httpserver.HttpServer;
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

    // The Shared "Time" State
    private final AtomicLong globalSnapshotId = new AtomicLong(0);

    public GatewayApp(Function<String, CoordinatorClient> coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
    }

    // UPDATED SIGNATURE: Takes two URLs now
    public HttpServer start(String bindHost, int port, String httpUrl, String sseUrl) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);

        // 1. Start Radio Receiver on Port 8091 (SSE URL)
        startSnapshotMonitor(sseUrl);

        // 2. Initialize Client on Port 8079 (HTTP URL)
        CoordinatorClient coordinatorClient = coordinatorClientFactory.apply(httpUrl);

        // 3. Create Service with access to the Snapshot ID
        OlapGatewayService service = new OlapGatewayService(coordinatorClient, globalSnapshotId);

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
            // Pass the host and port from the SSE URL (8091)
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