package dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog;

import java.util.List;

public record CatalogTable(

        String tableName,
        List<CatalogColumn> columns,
        int[] primaryKeyColumns
){}
