package dk.ku.di.dms.vms.calcite.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/**
 * Immutable configuration for the Calcite gateway server, loaded once at
 * startup via {@link #load()}. Resolves values in priority order: defaults,
 * then {@code application.properties} (filesystem or classpath), then
 * environment variables. Exposes the bind host, port, thread count, and
 * the coordinator HTTP and SSE URLs consumed by the gateway's network and
 * commit-notification components.
 */
public final class GatewayConfig {

    private final String bindHost;
    private final int    port;
    private final String coordinatorHttpUrl;
    private final String coordinatorSseUrl;
    private final int    gatewayThreads;

    private GatewayConfig(String bindHost, int port,
                          String coordinatorHttpUrl, String coordinatorSseUrl,
                          int gatewayThreads) {
        this.bindHost           = Objects.requireNonNull(bindHost);
        this.port               = port;
        this.coordinatorHttpUrl = Objects.requireNonNull(coordinatorHttpUrl);
        this.coordinatorSseUrl  = Objects.requireNonNull(coordinatorSseUrl);
        this.gatewayThreads     = gatewayThreads;
    }

    public static GatewayConfig load() {
        String bindHost = "0.0.0.0";
        int    port     = 8095;
        String httpUrl  = "http://localhost:8079";
        String sseUrl   = "http://localhost:8091";
        int    threads  = 4;

        Properties props = loadProperties();
        bindHost = props.getProperty("gateway.bind_host", bindHost);
        port     = parseInt(props.getProperty("gateway.port"),     port);
        threads  = parseInt(props.getProperty("gateway.threads"),  threads);
        httpUrl  = props.getProperty("coordinator.http_url", httpUrl);
        sseUrl   = props.getProperty("coordinator.sse_url",  sseUrl);

        bindHost = getEnv("BIND_HOST",            bindHost);
        port     = parseInt(System.getenv("GATEWAY_PORT"),    port);
        threads  = parseInt(System.getenv("GATEWAY_THREADS"), threads);
        httpUrl  = getEnv("COORDINATOR_HTTP_URL", httpUrl);
        sseUrl   = getEnv("COORDINATOR_SSE_URL",  sseUrl);

        System.out.println("[GatewayConfig] bindHost=" + bindHost
                + " port=" + port
                + " threads=" + threads
                + " httpUrl=" + httpUrl
                + " sseUrl=" + sseUrl);

        return new GatewayConfig(bindHost, port, httpUrl, sseUrl, threads);
    }

    public String getBindHost()           { return bindHost; }
    public Integer    getPort()               { return port; }
    public String getCoordinatorHttpUrl() { return coordinatorHttpUrl; }
    public String getCoordinatorSseUrl()  { return coordinatorSseUrl; }
    public Integer    getGatewayThreads()     { return gatewayThreads; }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        InputStream inputStream = openProperties();
        if (inputStream == null) {
            System.out.println("[GatewayConfig] No application.properties found — using defaults.");
            return properties;
        }
        try (inputStream) {
            properties.load(inputStream);
            System.out.println("[GatewayConfig] Loaded application.properties ("
                    + properties.size() + " keys).");
        } catch (IOException e) {
            System.err.println("[GatewayConfig] Failed to read application.properties: "
                    + e.getMessage());
        }
        return properties;
    }

    private static InputStream openProperties() {
        Path path = Paths.get("application.properties");
        if (Files.exists(path)) {
            try { return Files.newInputStream(path); } catch (IOException ignored) {}
        }
        return GatewayConfig.class.getClassLoader()
                .getResourceAsStream("application.properties");
    }

    private static String getEnv(String key, String definition) {
        String environment = System.getenv(key);
        return (environment == null || environment.isBlank()) ? definition : environment;
    }

    private static int parseInt(String environment, int definition) {
        if (environment == null || environment.isBlank()) return definition;
        try { return Integer.parseInt(environment.trim()); } catch (Exception e) { return definition; }
    }

    @Override
    public String toString() {
        return "GatewayConfig{bindHost='" + bindHost + "', port=" + port
                + ", threads=" + gatewayThreads
                + ", httpUrl='" + coordinatorHttpUrl
                + "', sseUrl='" + coordinatorSseUrl + "'}";
    }
}