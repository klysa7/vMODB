package dk.ku.di.dms.vms.calcite.client;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;

public record ColumnDescriptor(
        String     name,
        CatalogType type,
        int        byteOffset,
        int        byteSize
) {
    public boolean isReadable() {
        return byteSize > 0;
    }
}