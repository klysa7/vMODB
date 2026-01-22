package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class CoordinatorCatalog {

    private final Map<String, Map<String, CatalogTable>> schemas = new HashMap<>();
    private final Map<TableId, String> placement = new HashMap<>();
    private volatile long snapshotId = -1L;


    public long getSnapshotId() {
        return snapshotId;
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

    public record TableId(String schema, String table) {}
}
