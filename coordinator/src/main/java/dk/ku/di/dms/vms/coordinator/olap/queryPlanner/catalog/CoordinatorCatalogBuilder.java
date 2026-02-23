package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog;

import dk.ku.di.dms.vms.modb.common.schema.VmsDataModel;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public final class CoordinatorCatalogBuilder {

    public CoordinatorCatalog buildFromVmsMetadata(Map<String, VmsNode> vmsMetadataMap) {
        CoordinatorCatalog catalog = new CoordinatorCatalog();

        vmsMetadataMap.values().forEach(vmsNode -> {
            String ownerVmsSchema = vmsNode.identifier;

            vmsNode.dataSchema.values().forEach(model -> {
                CatalogTable table = toCatalogTable(model);
                catalog.addTable(ownerVmsSchema, table, vmsNode.identifier);
            });
        });
        return catalog;
    }

    private CatalogTable toCatalogTable(VmsDataModel model) {
        List<CatalogColumn> columns = new ArrayList<>(model.columnNames.length);

        IntStream.range(0, model.columnNames.length).forEach(i -> {
            String columnName = model.columnNames[i];
            DataType dataType = model.columnDataTypes[i];

            boolean nullable = true;

            columns.add(new CatalogColumn(
                    columnName,
                    mapType(dataType),
                    nullable
            ));
        });

        int[] primaryKeyColumns =
                (model.primaryKeyColumns == null)
                        ? new int[0]
                        : model.primaryKeyColumns;

        return new CatalogTable(model.tableName, columns, primaryKeyColumns);
    }

    private CatalogType mapType(DataType dataType) {
        if (dataType == null) {
            return CatalogType.BYTES;
        }

        return switch (dataType) {
            case BOOL -> CatalogType.BOOLEAN;
            case INT -> CatalogType.INT;
            case LONG -> CatalogType.BIGINT;
            case FLOAT -> CatalogType.FLOAT;
            case DOUBLE -> CatalogType.DOUBLE;
            case CHAR, STRING, ENUM -> CatalogType.VARCHAR;
            case DATE -> CatalogType.DATE;
            case STRING_ARRAY,
                 FLOAT_ARRAY,
                 INT_ARRAY,
                 COMPLEX -> CatalogType.BYTES;
        };
    }
}