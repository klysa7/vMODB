package dk.ku.di.dms.vms.calcite.client;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;

/**
 * Describes one column in the byte array received from a VMS over TCP.
 *
 * byteOffset: data-relative position of this column's first byte in the row payload
 *             (i.e. after Schema.RECORD_HEADER has already been stripped by the VMS).
 *             Computed as the cumulative sum of byteSize() of all preceding columns.
 *
 * byteSize:   number of bytes this column occupies. For VARCHAR this must come from
 *             CatalogColumnDto.byteSize() (the actual CHAR width declared in the VMS schema).
 *             If 0, VmsResultIterator outputs null for this column.
 *
 * type:       used by VmsResultIterator to choose the correct read method (getInt, getLong, etc.)
 */
public record ColumnDescriptor(
        String     name,
        CatalogType type,
        int        byteOffset,
        int        byteSize
) {
    /** Returns true when VmsResultIterator can meaningfully read this column. */
    public boolean isReadable() {
        return byteSize > 0;
    }
}