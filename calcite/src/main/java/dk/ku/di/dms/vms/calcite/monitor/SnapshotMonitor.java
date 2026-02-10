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
 * LISTENER for vMODB Coordinator.
 * Connects to the SSE stream and updates the Global Snapshot ID.
 */
public class SnapshotMonitor implements Runnable {

    private final String coordinatorUrl;
    private final AtomicLong globalSnapshotId; // Shared State

    // Use a single client to reuse resources
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public SnapshotMonitor(String coordinatorHost, int coordinatorPort, AtomicLong globalSnapshotId) {
        // Use the C# convention: /status/committed
        this.coordinatorUrl = "http://" + coordinatorHost + ":" + coordinatorPort + "/status/committed";
        this.globalSnapshotId = globalSnapshotId;
    }

    @Override
    public void run() {
        System.out.println("[SnapshotMonitor] Connecting to Coordinator at: " + coordinatorUrl);

        // Infinite Loop: If the Coordinator crashes/restarts, we reconnect automatically.
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
                .header("Accept", "text/event-stream") // <--- TRIGGERS THE BROADCAST
                .timeout(Duration.ofMinutes(60))       // Keep connection alive
                .GET()
                .build();

        // Send request, but don't read the whole body (it's infinite). Get an InputStream.
        HttpResponse<java.io.InputStream> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofInputStream());

        if (response.statusCode() != 200) {
            throw new RuntimeException("Server returned status: " + response.statusCode());
        }

        System.out.println("[SnapshotMonitor] Connected! Waiting for broadcasts...");

        // Read line-by-line
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.body()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Protocol: "data: 10050"
                if (!line.isEmpty() && line.startsWith("data:")) {
                    String data = line.substring(5).trim(); // Remove "data:"
                    try {
                        long newTid = Long.parseLong(data);

                        // ATOMIC UPDATE
                        long oldTid = globalSnapshotId.getAndSet(newTid);

                        if (newTid > oldTid) {
                            System.out.println("[SnapshotMonitor] New Global Snapshot: " + newTid);
                        }
                    } catch (NumberFormatException e) {
                        // Ignore malformed lines
                    }
                }
            }
        }
    }
}