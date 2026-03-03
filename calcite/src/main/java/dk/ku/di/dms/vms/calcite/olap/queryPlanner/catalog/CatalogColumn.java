package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;


public record CatalogColumn (
    String name,
    CatalogType type,
    boolean nullable,
    int byteSize
){}
