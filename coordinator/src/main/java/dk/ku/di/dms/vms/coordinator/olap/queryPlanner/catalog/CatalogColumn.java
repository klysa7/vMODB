package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog;

public record CatalogColumn (
    String name,
    CatalogType type,
    boolean nullable
){}
