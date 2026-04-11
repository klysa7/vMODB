package dk.ku.di.dms.vms.calcite.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

/**
 * Gateway configuration.
 *
 * Load order (later overrides earlier):
 *   1. Hardcoded defaults
 *   2. application.properties (working directory, then classpath)
 *   3. Environment variables (for CI/Docker overrides)
 *
 * application.properties:
 * ─────────────────────────────────────────────
 * gateway.bind_host=0.0.0.0
 * gateway.port=8095
 * gateway.threads=4
 * coordinator.http_url=http://localhost:8079
 * coordinator.sse_url=http://localhost:8091
 * ─────────────────────────────────────────────
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

    // ── Load ─────────────────────────────────────────────────────────────────

    public static GatewayConfig load() {
        // Step 1: hardcoded defaults
        String bindHost = "0.0.0.0";
        int    port     = 8095;
        String httpUrl  = "http://localhost:8079";
        String sseUrl   = "http://localhost:8091";
        int    threads  = 4;

        // Step 2: application.properties overrides defaults
        Properties props = loadProperties();
        bindHost = props.getProperty("gateway.bind_host", bindHost);
        port     = parseInt(props.getProperty("gateway.port"),     port);
        threads  = parseInt(props.getProperty("gateway.threads"),  threads);
        httpUrl  = props.getProperty("coordinator.http_url", httpUrl);
        sseUrl   = props.getProperty("coordinator.sse_url",  sseUrl);

        // Step 3: env vars override properties (CI/Docker use case)
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

    /** Backward-compatible alias. */
    public static GatewayConfig fromEnv() {
        return load();
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    public String getBindHost()           { return bindHost; }
    public int    getPort()               { return port; }
    public String getCoordinatorHttpUrl() { return coordinatorHttpUrl; }
    public String getCoordinatorSseUrl()  { return coordinatorSseUrl; }
    public int    getGatewayThreads()     { return gatewayThreads; }

    // ── Properties loader ─────────────────────────────────────────────────────
    //
    // Search order:
    //   1. ./application.properties (working directory) — edit without rebuild
    //   2. classpath:/application.properties (packaged in jar)

    private static Properties loadProperties() {
        Properties props = new Properties();
        InputStream stream = openProperties();
        if (stream == null) {
            System.out.println("[GatewayConfig] No application.properties found — using defaults.");
            return props;
        }
        try (stream) {
            props.load(stream);
            System.out.println("[GatewayConfig] Loaded application.properties ("
                    + props.size() + " keys).");
        } catch (IOException e) {
            System.err.println("[GatewayConfig] Failed to read application.properties: "
                    + e.getMessage());
        }
        return props;
    }

    private static InputStream openProperties() {
        // 1. Working directory — edit without repackaging
        Path workDir = Paths.get("application.properties");
        if (Files.exists(workDir)) {
            try { return Files.newInputStream(workDir); } catch (IOException ignored) {}
        }
        // 2. Classpath — packaged inside the jar
        return GatewayConfig.class.getClassLoader()
                .getResourceAsStream("application.properties");
    }

    private static String getEnv(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    private static int parseInt(String v, int def) {
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    @Override
    public String toString() {
        return "GatewayConfig{bindHost='" + bindHost + "', port=" + port
                + ", threads=" + gatewayThreads
                + ", httpUrl='" + coordinatorHttpUrl
                + "', sseUrl='" + coordinatorSseUrl + "'}";
    }
}