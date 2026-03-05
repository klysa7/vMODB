package dk.ku.di.dms.vms.modb.common.coordinator.api;

/**
 * Wire DTO for a single column in the coordinator catalog REST response.
 *
 * byteSize: the exact number of bytes this column occupies in the VMS off-heap
 *   layout (= DataType.value for the column's type).
 *   For fixed-width types the coordinator can derive this from the DataType enum.
 *   For VARCHAR/CHAR types the coordinator must set this from the actual declared
 *   column width (e.g. CHAR(16) → 16, CHAR(500) → 500).
 *   VmsResultIterator uses this to navigate column boundaries in the raw byte array
 *   it receives over TCP without any Object[] materialization.
 */
public record CatalogColumnDto(
        String name,
        String type,
        int byteSize
) {
    /** Backward-compatible factory for callers that pre-date the byteSize field. */
    public static CatalogColumnDto of(String name, String type) {
        return new CatalogColumnDto(name, type, 0);
    }
}