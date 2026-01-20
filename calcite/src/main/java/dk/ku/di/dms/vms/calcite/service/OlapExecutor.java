package dk.ku.di.dms.vms.calcite.service;

import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.SnapshotResponse;

public interface OlapExecutor {
    /**
     * Executes the SQL using the given snapshot + catalog.
     * Return value is JSON (same as you return today) to avoid changing semantics.
     */
    String execute(String sql, SnapshotResponse snapshot, CatalogResponse catalog);
}