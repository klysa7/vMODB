package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog;

import java.util.*;

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

    public Optional<CatalogTable> getTable(String schemaName, String tableName) {
        return Optional.ofNullable(schemas.getOrDefault(schemaName, Map.of()).get(tableName));
    }

    public Map<String, CatalogTable> tablesInSchema(String schemaName) {
        return Collections.unmodifiableMap(schemas.getOrDefault(schemaName, Map.of()));
    }

    public Set<String> schemaNames() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    public String ownerOf(String schemaName, String tableName) {
        return placement.get(new TableId(schemaName, tableName));
    }

    public record TableId(String schema, String table) {}
}
