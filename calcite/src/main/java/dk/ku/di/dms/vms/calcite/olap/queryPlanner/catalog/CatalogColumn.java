package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;


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