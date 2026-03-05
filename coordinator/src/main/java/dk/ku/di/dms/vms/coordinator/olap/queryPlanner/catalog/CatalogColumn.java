package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog;

/**
 * Immutable descriptor for a single column as seen by the coordinator/gateway.
 * See calcite-CatalogColumn.java for full Javadoc.
 */
public record CatalogColumn(
        String name,
        CatalogType type,
        boolean nullable,
        int storedByteSize
) {
    public CatalogColumn(String name, CatalogType type, boolean nullable) {
        this(name, type, nullable, 0);
    }

    public int byteSize() {
        return switch (type) {
            case INT                   -> 4;
            case LONG, BIGINT          -> 8;
            case FLOAT                 -> 4;
            case DOUBLE                -> 8;
            case BOOLEAN, BOOL         -> 1;
            case DATE, TIMESTAMP       -> 8;
            case VARCHAR, STRING, BYTES -> storedByteSize;
            default                    -> storedByteSize;
        };
    }
}