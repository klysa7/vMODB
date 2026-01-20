package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog;

import java.util.List;

public record CatalogTable(

        String tableName,
        List<CatalogColumn> columns,
        int[] primaryKeyColumns
){}
