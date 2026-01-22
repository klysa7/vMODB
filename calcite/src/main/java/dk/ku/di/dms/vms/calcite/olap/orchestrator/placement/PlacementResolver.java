package dk.ku.di.dms.vms.calcite.olap.orchestrator.placement;

public final class PlacementResolver {

    public String endpointUrl(String schema, String table) {

        if ("order".equals(schema) && "orders".equals(table)) {
            return "http://localhost:8083/orders";
        }

        if ("payment".equals(schema) && "order_payment_cards".equals(table)) {
            return "http://localhost:8084/order-payments";
        }

        throw new IllegalArgumentException("No endpoint mapping for " + schema + "." + table);
    }

    public String ownerVms(String schema, String table) {
        return schema;
    }
}