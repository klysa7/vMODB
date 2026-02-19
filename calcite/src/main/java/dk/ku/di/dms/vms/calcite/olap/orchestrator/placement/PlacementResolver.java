package dk.ku.di.dms.vms.calcite.olap.orchestrator.placement;

public final class PlacementResolver {

    public String endpointUrl(String schema, String table) {

        if ("warehouse".equals(schema)) {
            return "http://localhost:8001/" + table;
        }

        if ("inventory".equals(schema)) {
            return "http://localhost:8002/" + table;
        }

        if ("order".equals(schema)) {
            return "http://localhost:8003/" + table;
        }

        if ("payment".equals(schema)) {
            return "http://localhost:8084/" + table;
        }

        throw new IllegalArgumentException("No endpoint mapping for " + schema + "." + table);
    }

    public String ownerVms(String schema, String table) {
        return schema;
    }
}