package dk.ku.di.dms.vms.calcite.monitor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background thread that subscribes to the coordinator's SSE endpoint
 * ({@code /status/committed}) and advances the gateway's global snapshot ID
 * whenever a new batch commits. The updated ID is consumed by
 * {@link dk.ku.di.dms.vms.calcite.service.OlapGatewayService} to ensure
 * analytical queries always execute against the freshest committed snapshot.
 * Reconnects automatically on connection loss with a 2-second backoff.
 */
public class SnapshotMonitor implements Runnable {

    private final String coordinatorUrl;
    private final AtomicLong globalSnapshotId;
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public SnapshotMonitor(String coordinatorHost, int coordinatorPort, AtomicLong globalSnapshotId) {
        this.coordinatorUrl = "http://" + coordinatorHost + ":" + coordinatorPort + "/status/committed";
        this.globalSnapshotId = globalSnapshotId;
    }

    @Override
    public void run() {
        System.out.println("[SnapshotMonitor] Connecting to Coordinator at: " + coordinatorUrl);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                connectAndListen();
            } catch (Exception e) {
                System.err.println("[SnapshotMonitor] Connection lost (" + e.getMessage() + "). Retrying in 2s...");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private void connectAndListen() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(coordinatorUrl))
                .header("Accept", "text/event-stream")
                .timeout(Duration.ofMinutes(60))
                .GET()
                .build();

        HttpResponse<java.io.InputStream> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Server returned status: " + response.statusCode());
        }

        System.out.println("[SnapshotMonitor] Connected! Waiting for broadcasts...");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty() && line.startsWith("data:")) {
                    String data = line.substring(5).trim();
                    try {
                        long newTid = Long.parseLong(data);

                        long oldTid = globalSnapshotId.getAndSet(newTid);

                        if (newTid > oldTid) {
                            System.out.println("[SnapshotMonitor] New Global Snapshot: " + newTid);
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            }
        }
    }
}