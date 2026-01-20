package dk.ku.di.dms.vms.modb.common.coordinator.api;

import java.util.List;
import java.util.Map;

public record CatalogResponse(
        long snapshotId,
        Map<String, Map<String, CatalogTableDto>> schemas,
        List<PlacementDto> placement
) {}
