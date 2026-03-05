package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;

/**
 * Immutable descriptor for a single column as seen by the coordinator/gateway.
 *
 * storedByteSize: the exact byte width of this column in the VMS off-heap layout.
 *   - For INT/LONG/FLOAT/DOUBLE/BOOL: derived from the type alone (always correct).
 *   - For VARCHAR/CHAR: must come from CatalogColumnDto.byteSize(), which the
 *     coordinator populates from DataType.value of the actual VMS schema column.
 *     If the coordinator sends 0 (old coordinator, no upgrade yet), byteSize()
 *     returns 0 and that column will be output as null by VmsResultIterator —
 *     which is safe and makes the gap visible rather than silently wrong.
 */
public record CatalogColumn(
        String name,
        CatalogType type,
        boolean nullable,
        int storedByteSize
) {
    /** Backward-compatible constructor for callers that don't have a stored size. */
    public CatalogColumn(String name, CatalogType type, boolean nullable) {
        this(name, type, nullable, 0);
    }

    /**
     * Returns the number of bytes this column occupies in the VMS off-heap data payload.
     * For fixed-width numeric types this is derived from the type alone.
     * For VARCHAR/CHAR this returns storedByteSize (set from CatalogColumnDto.byteSize).
     */
    public int byteSize() {
        return switch (type) {
            case INT                   -> 4;
            case LONG, BIGINT          -> 8;
            case FLOAT                 -> 4;
            case DOUBLE                -> 8;
            case BOOLEAN, BOOL         -> 1;
            case DATE, TIMESTAMP       -> 8;  // stored as epoch LONG in VMS
            // VARCHAR/STRING/CHAR: use the declared width from the coordinator catalog.
            // Returns 0 if the coordinator has not yet been updated to send byteSize.
            case VARCHAR, STRING, BYTES -> storedByteSize;
            default                    -> storedByteSize;
        };
    }
}