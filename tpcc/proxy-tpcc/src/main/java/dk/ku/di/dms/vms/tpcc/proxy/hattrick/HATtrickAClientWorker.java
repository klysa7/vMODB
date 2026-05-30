package dk.ku.di.dms.vms.tpcc.proxy.hattrick;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HATtrick A-client). Repeatedly issues GET requests to the gateway
 * for the configured query path and counts successful responses.
 * The runner samples getCompletedCount() at the window edges to compute A-qps.
 */
public final class HATtrickAClientWorker implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(HATtrickAClientWorker.class.getName());

    private final int           clientId;
    private final String        queryUrl;
    private final AtomicBoolean running;
    private final AtomicLong    completed = new AtomicLong(0L);

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    public HATtrickAClientWorker(int clientId,
                                 String gatewayBaseUrl,
                                 String queryPath,
                                 AtomicBoolean running) {
        this.clientId  = clientId;
        this.queryUrl  = gatewayBaseUrl + queryPath;
        this.running   = running;
    }

    @Override
    public void run() {
//        LOG.log(System.Logger.Level.INFO,
//                "A-client {0} started -> {1}", clientId, queryUrl);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(queryUrl))
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
//                    LOG.log(System.Logger.Level.WARNING,
//                            "A-client {0}: gateway returned HTTP {1}",
//                            clientId, resp.statusCode());
                    Thread.sleep(200);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (!running.get()) break;
//                LOG.log(System.Logger.Level.WARNING,
//                        "A-client {0} query error: {1}", clientId, e.getMessage());
                try { Thread.sleep(100); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

//        LOG.log(System.Logger.Level.INFO,
//                "A-client {0} stopped. Completed {1} queries",
//                clientId, completed.get());
    }

    public long getCompletedCount() { return completed.get(); }
}