package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick A-client for the throughput frontier experiment.
 *
 * Issues GET /olap/q1 requests to the Calcite gateway in a tight loop.
 * Q1 = count of orders per district (GROUP BY c_d_id COUNT(*)) — the
 * analytical query that requires the distributed broadcast hash join
 * between customer (warehouse VMS) and orders (order VMS).
 *
 * Throughput is measured by the caller via getCompletedCount() sampled
 * at the start and end of the measurement window.
 *
 * No freshness measurement — the professor confirmed it is not needed
 * because vMODB always reads live MVCC data (freshness = 0 by design).
 */
public final class HATtrickAClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickAClientWorker.class.getName());

    private final int           clientId;
    private final String        q1Url;       // e.g. "http://localhost:8095/olap/q1"
    private final AtomicBoolean running;

    // completed query count — incremented on every successful HTTP 200
    private final AtomicLong completed = new AtomicLong(0L);

    // one HttpClient per worker — reuses persistent connections (HTTP/1.1
    // keep-alive) so TCP overhead is not measured in the QPS number
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();

    public HATtrickAClientWorker(int clientId,
                                 String gatewayBaseUrl,
                                 AtomicBoolean running) {
        this.clientId = clientId;
        this.q1Url    = gatewayBaseUrl + "/olap/q1";
        this.running  = running;
    }

    @Override
    public void run() {
        LOG.log(System.Logger.Level.INFO,
                "A-client {0} started -> {1}", clientId, q1Url);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(q1Url))
                .header("Accept", "application/json")
                .GET()
                .build();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                HttpResponse<String> resp =
                        http.send(request, HttpResponse.BodyHandlers.ofString());

                if (resp.statusCode() == 200) {
                    completed.incrementAndGet();
                } else {
                    LOG.log(System.Logger.Level.WARNING,
                            "A-client {0}: gateway returned HTTP {1}",
                            clientId, resp.statusCode());
                    // brief pause to avoid hammering a failing gateway
                    Thread.sleep(200);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running.get()) break; // normal shutdown
                LOG.log(System.Logger.Level.WARNING,
                        "A-client {0} query error: {1}", clientId, e.getMessage());
                try { Thread.sleep(100); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        LOG.log(System.Logger.Level.INFO,
                "A-client {0} stopped. Completed {1} queries",
                clientId, completed.get());
    }

    /** Snapshot of completed query count — call at start and end of
     *  measurement window and subtract to get queries in window. */
    public long getCompletedCount() {
        return completed.get();
    }
}