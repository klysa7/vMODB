package dk.ku.di.dms.vms.modb.common.coordinator.api;

public record PlacementDto(
        String schema,
        String table,
        String ownerVms
) {}