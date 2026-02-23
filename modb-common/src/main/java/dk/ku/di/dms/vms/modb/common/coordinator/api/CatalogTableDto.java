package dk.ku.di.dms.vms.modb.common.coordinator.api;


import java.util.List;

public record CatalogTableDto(
        String ownerVms,
        List<CatalogColumnDto> columns
) {}
