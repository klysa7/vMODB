package dk.ku.di.dms.vms.tpcc.proxy.infra;

import java.util.HashMap;
import java.util.Map;

public final class TPCcConstants {

    /**
     * WORKLOAD
     */
    public static final boolean FORCE_ABORTS = false;

    /**
     * DATA
     */
    public static final int NUM_DIST_PER_WARE = 10;
    public static final int NUM_CUST_PER_DIST = 3000;
    public static final int NUM_ITEMS = 100000;
    public static final int MIN_NUM_ITEMS_PER_ORDER = 5;
    public static final int MAX_NUM_ITEMS_PER_ORDER = 15;

    /**
     * PORTS
     */
    public static final int WAREHOUSE_VMS_PORT = 8001;

    public static final int INVENTORY_VMS_PORT = 8002;

    public static final int ORDER_VMS_PORT = 8003;

    public static final Map<String, Integer> VMS_TO_PORT_MAP;

    public static final Map<String, String> TABLE_TO_VMS_MAP;

    static {
        VMS_TO_PORT_MAP = new HashMap<>(3);
        VMS_TO_PORT_MAP.put("warehouse", WAREHOUSE_VMS_PORT);
        VMS_TO_PORT_MAP.put("inventory", INVENTORY_VMS_PORT);
        VMS_TO_PORT_MAP.put("order", ORDER_VMS_PORT);

        TABLE_TO_VMS_MAP = new HashMap<>();
        TABLE_TO_VMS_MAP.put("warehouse", "warehouse");
        TABLE_TO_VMS_MAP.put("district", "warehouse");
        TABLE_TO_VMS_MAP.put("customer", "warehouse");

        TABLE_TO_VMS_MAP.put("item", "inventory");
        TABLE_TO_VMS_MAP.put("stock", "inventory");

        TABLE_TO_VMS_MAP.put("orders", "order");
        TABLE_TO_VMS_MAP.put("new_orders", "order");
        TABLE_TO_VMS_MAP.put("order_line", "order");
    }

}