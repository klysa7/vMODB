package dk.ku.di.dms.vms.tpcc.proxy.dataload;

import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public final class DataLoadUtils {

    private static final System.Logger LOGGER = System.getLogger(DataLoadUtils.class.getName());

    public static final int WAREHOUSE_VMS_PORT = 8001;
    public static final int INVENTORY_VMS_PORT = 8002;
    public static final int ORDER_VMS_PORT     = 8003;
    public static final int REPLICA_VMS_PORT   = 8004;

    public static final Map<String, Integer> VMS_TO_PORT_MAP;
    public static final Map<String, String>  VMS_TO_HOST_MAP;

    static {
        Properties properties = ConfigUtils.loadProperties();

        VMS_TO_HOST_MAP = new HashMap<>(4);
        VMS_TO_HOST_MAP.put("warehouse", properties.getProperty("warehouse_host"));
        VMS_TO_HOST_MAP.put("inventory", properties.getProperty("inventory_host"));
        VMS_TO_HOST_MAP.put("order",     properties.getProperty("order_host"));
        // replica_host defaults to localhost when not set
        VMS_TO_HOST_MAP.put("replica",
                properties.getProperty("replica_host", "localhost"));

        VMS_TO_PORT_MAP = new HashMap<>(4);
        VMS_TO_PORT_MAP.put("warehouse", WAREHOUSE_VMS_PORT);
        VMS_TO_PORT_MAP.put("inventory", INVENTORY_VMS_PORT);
        VMS_TO_PORT_MAP.put("order",     ORDER_VMS_PORT);
        VMS_TO_PORT_MAP.put("replica",   REPLICA_VMS_PORT);
    }

    private static final Map<String, ConcurrentLinkedDeque<MinimalHttpClient>> CONNECTION_POOL =
            new ConcurrentHashMap<>();

    public static void releaseAllConnections() {
        for (var entries : CONNECTION_POOL.values()) {
            for (var conn : entries) {
                conn.close();
            }
        }
        CONNECTION_POOL.clear();
    }

    public static MinimalHttpClient obtainHttpClient(String vms) {
        var clientPool = CONNECTION_POOL.computeIfAbsent(
                vms, (ignored) -> new ConcurrentLinkedDeque<>());
        if (!clientPool.isEmpty()) {
            MinimalHttpClient client = clientPool.poll();
            if (client != null) return client;
        }
        try {
            String host = VMS_TO_HOST_MAP.get(vms);
            int    port = VMS_TO_PORT_MAP.get(vms);
            return new MinimalHttpClient(host, port);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Exception captured for VMS " + vms + ":\n" + e);
        }
    }

    public static void returnHttpClient(String vms, MinimalHttpClient client) {
        CONNECTION_POOL.get(vms).add(client);
    }

    public static void cleanup(boolean reset) {
        String param = reset ? "reset" : "cleanup";
        Properties properties = ConfigUtils.loadProperties();
        for (Map.Entry<String, Integer> vms : VMS_TO_PORT_MAP.entrySet()) {
            // skip replica — it has no cleanup endpoint
            if (vms.getKey().equals("replica")) continue;
            String host = properties.getProperty(vms.getKey() + "_host");
            try (MinimalHttpClient client = new MinimalHttpClient(host, vms.getValue())) {
                if (client.sendRequest("PATCH", "", param) != 200) {
                    System.out.println("Error on resetting " + vms.getKey() + " state!");
                }
            } catch (IOException e) {
                System.out.println("Exception on resetting " + vms.getKey() + " state:\n" + e);
            }
        }
    }
}