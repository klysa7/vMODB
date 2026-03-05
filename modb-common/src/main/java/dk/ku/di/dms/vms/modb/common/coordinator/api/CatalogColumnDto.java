package dk.ku.di.dms.vms.modb.common.coordinator.api;

public record CatalogColumnDto(
        String name,
        String type,
        int byteSize
) {
    public static CatalogColumnDto of(String name, String type) {
        return new CatalogColumnDto(name, type, 0);
    }
}