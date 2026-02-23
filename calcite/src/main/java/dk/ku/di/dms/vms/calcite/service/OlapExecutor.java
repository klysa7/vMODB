package dk.ku.di.dms.vms.calcite.service;

import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.SnapshotResponse;

public interface OlapExecutor {

    String execute(String sql, SnapshotResponse snapshot, CatalogResponse catalog);
}