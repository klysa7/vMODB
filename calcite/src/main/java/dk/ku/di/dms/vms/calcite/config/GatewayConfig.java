package dk.ku.di.dms.vms.calcite.config;

import java.util.Map;
import java.util.Objects;

public final class GatewayConfig {

    private final String bindHost;
    private final int port;
    private final String coordinatorUrl;

    private GatewayConfig(String bindHost, int port, String coordinatorUrl) {
        this.bindHost = Objects.requireNonNull(bindHost, "bindHost");
        this.port = port;
        this.coordinatorUrl = Objects.requireNonNull(coordinatorUrl, "coordinatorUrl");
    }

    public static GatewayConfig fromEnv() {
        Map<String, String> env = System.getenv();

        String bindHost = env.getOrDefault("BIND_HOST", "0.0.0.0");
        int port = parseInt(env.get("GATEWAY_PORT"), 8095);
        String coordinatorUrl = env.getOrDefault("COORDINATOR_URL", "http://localhost:8079");

        return new GatewayConfig(bindHost, port, coordinatorUrl);
    }

    public String getBindHost() {
        return bindHost;
    }

    public int getPort() {
        return port;
    }

    public String getCoordinatorUrl() {
        return coordinatorUrl;
    }

    private static int parseInt(String v, int def) {
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (Exception e) {
            return def;
        }
    }

    @Override
    public String toString() {
        return "GatewayConfig{" +
                "bindHost='" + bindHost + '\'' +
                ", port=" + port +
                ", coordinatorUrl='" + coordinatorUrl + '\'' +
                '}';
    }
}