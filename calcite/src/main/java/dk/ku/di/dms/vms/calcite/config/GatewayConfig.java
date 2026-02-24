package dk.ku.di.dms.vms.calcite.config;

import java.util.Map;
import java.util.Objects;

public final class GatewayConfig {

    private final String bindHost;
    private final int port;
    private final String coordinatorHttpUrl;
    private final String coordinatorSseUrl;

    private GatewayConfig(String bindHost, int port, String coordinatorHttpUrl, String coordinatorSseUrl) {
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.port = port;
        this.coordinatorHttpUrl = Objects.requireNonNull(coordinatorHttpUrl, "coordinatorHttpUrl");
        this.coordinatorSseUrl = Objects.requireNonNull(coordinatorSseUrl, "coordinatorSseUrl");
    }

    public static GatewayConfig fromEnv() {
        Map<String, String> env = System.getenv();

        String bindHost = env.getOrDefault("BIND_HOST", "0.0.0.0");
        int port = parseInt(env.get("GATEWAY_PORT"), 8095);

        String httpUrl = env.getOrDefault("COORDINATOR_HTTP_URL", "http://localhost:8079");

        String sseUrl = env.getOrDefault("COORDINATOR_SSE_URL", "http://localhost:8091");

        return new GatewayConfig(bindHost, port, httpUrl, sseUrl);
    }

    public String getBindHost() { return bindHost; }
    public int getPort() { return port; }

    public String getCoordinatorHttpUrl() { return coordinatorHttpUrl; }
    public String getCoordinatorSseUrl() { return coordinatorSseUrl; }

    private static int parseInt(String v, int def) {
        if (v == null || v.isBlank()) return def;
        try { return Integer.parseInt(v.trim()); } catch (Exception e) { return def; }
    }

    @Override
    public String toString() {
        return "GatewayConfig{" +
                "bindHost='" + bindHost + '\'' +
                ", port=" + port +
                ", httpUrl='" + coordinatorHttpUrl + '\'' +
                ", sseUrl='" + coordinatorSseUrl + '\'' +
                '}';
    }
}