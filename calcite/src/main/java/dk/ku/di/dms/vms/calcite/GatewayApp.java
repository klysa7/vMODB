package dk.ku.di.dms.vms.calcite;

import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.config.GatewayConfig;
import dk.ku.di.dms.vms.calcite.http.GatewayHttpHandler;
import dk.ku.di.dms.vms.calcite.monitor.SnapshotMonitor;
import dk.ku.di.dms.vms.calcite.service.CoordinatorClient;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public final class GatewayApp {

    private final Function<String, CoordinatorClient> coordinatorClientFactory;
    private final AtomicLong globalSnapshotId = new AtomicLong(0);

    public GatewayApp(Function<String, CoordinatorClient> coordinatorClientFactory) {
        this.coordinatorClientFactory = coordinatorClientFactory;
    }

    public HttpServer start(String bindHost, int port, String httpUrl, String sseUrl,
                            GatewayConfig config) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(bindHost, port), 0);

        startSnapshotMonitor(sseUrl);
        CoordinatorClient coordinatorClient = coordinatorClientFactory.apply(httpUrl);
        VmsGatewayClient gatewayClient = new VmsGatewayClient();

        OlapGatewayService service = new OlapGatewayService(
                coordinatorClient, globalSnapshotId, gatewayClient);

        server.createContext("/", new GatewayHttpHandler(service));

        // B1 FIX: thread pool size read from application.properties (gateway.threads).
        // BEFORE: Executors.newFixedThreadPool(4) hardcoded — cannot tune.
        // AFTER:  configurable via application.properties, default 4 (backward compatible).
        // Platform threads only — virtual threads deferred as future work.
        server.setExecutor(Executors.newFixedThreadPool(
                config.getGatewayThreads(),
                r -> {
                    Thread t = new Thread(r, "gateway-worker");
                    t.setDaemon(false);
                    return t;
                }
        ));

        server.start();
        System.out.println("Gateway started on " + bindHost + ":" + port);
        System.out.println(" - Catalog Client: " + httpUrl);
        System.out.println(" - Snapshot Monitor: " + sseUrl);
        System.out.println(" - Thread pool: " + config.getGatewayThreads() + " threads");

        return server;
    }

    /** Backward-compatible overload. */
    public HttpServer start(String bindHost, int port,
                            String httpUrl, String sseUrl) throws IOException {
        return start(bindHost, port, httpUrl, sseUrl, GatewayConfig.load());
    }

    private void startSnapshotMonitor(String sseUrl) {
        try {
            URI uri = URI.create(sseUrl);
            SnapshotMonitor monitor = new SnapshotMonitor(
                    uri.getHost(), uri.getPort(), this.globalSnapshotId);
            Thread monitorThread = new Thread(monitor);
            monitorThread.setName("Gateway-SnapshotMonitor");
            monitorThread.setDaemon(true);
            monitorThread.start();
        } catch (Exception e) {
            System.err.println("Failed to start SnapshotMonitor: " + e.getMessage());
        }
    }
}