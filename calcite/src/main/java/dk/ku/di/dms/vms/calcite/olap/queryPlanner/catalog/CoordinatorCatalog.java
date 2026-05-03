package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class CoordinatorCatalog {

    private final Map<String, Map<String, CatalogTable>> schemas = new HashMap<>();
    private final Map<TableId, String> placement = new HashMap<>();
    private volatile long snapshotId = -1L;
    private static final Map<String, String> SCHEMA_TO_ADDRESS = new HashMap<>();
    static {
        SCHEMA_TO_ADDRESS.put("warehouse", "localhost:8001");
        SCHEMA_TO_ADDRESS.put("inventory", "localhost:8002");
        SCHEMA_TO_ADDRESS.put("order",     "localhost:8003");
    }

    public void setSnapshotId(long snapshotId) {
        this.snapshotId = snapshotId;
    }

    public void addTable(String schemaName, CatalogTable table, String ownerVms) {
        schemas.computeIfAbsent(schemaName, __ -> new HashMap<>())
                .put(table.tableName(), table);
        placement.put(new TableId(schemaName, table.tableName()), ownerVms);
    }

    public Map<String, CatalogTable> tablesInSchema(String schemaName) {
        return Collections.unmodifiableMap(schemas.getOrDefault(schemaName, Map.of()));
    }

    public Set<String> getSchemaNames() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    public String getVmsAddress(String schema, String table) {
        String stored = placement.get(new TableId(schema, table));
        if (stored == null) {
            // No placement entry — try schema-level fallback
            stored = SCHEMA_TO_ADDRESS.get(schema);
            if (stored == null)
                throw new IllegalStateException(
                        "No VMS address for " + schema + "." + table
                                + ". Add it to CoordinatorCatalog.SCHEMA_TO_ADDRESS.");
            return stored;
        }
        // If the coordinator already sent a host:port (contains ":"), use it
        if (stored.contains(":")) return stored;
        // Otherwise it is a schema-name identifier — resolve via fallback
        String resolved = SCHEMA_TO_ADDRESS.get(stored);
        if (resolved == null)
            resolved = SCHEMA_TO_ADDRESS.get(schema); // try schema name directly
        if (resolved == null)
            throw new IllegalStateException(
                    "Cannot resolve VMS address for ownerVms='" + stored
                            + "' (table " + schema + "." + table + "). "
                            + "Add it to CoordinatorCatalog.SCHEMA_TO_ADDRESS.");
        return resolved;
    }

    public record TableId(String schema, String table) {}
}