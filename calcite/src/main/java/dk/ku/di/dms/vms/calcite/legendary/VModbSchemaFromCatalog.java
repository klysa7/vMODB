package dk.ku.di.dms.vms.calcite.legendary;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogTable;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.schema.vModbSchema;
import dk.ku.di.dms.vms.calcite.schema.vModbTable;
import org.apache.calcite.sql.type.SqlTypeName;

public final class VModbSchemaFromCatalog {

    private VModbSchemaFromCatalog() {}

    public static vModbSchema build(CoordinatorCatalog catalog, String schemaName) {
        vModbSchema.Builder sb = vModbSchema.newBuilder(schemaName);

        catalog.tablesInSchema(schemaName).forEach((_, t) -> {
            vModbTable.Builder tb = vModbTable.newBuilder(t.tableName());
            t.columns().forEach(c ->
                    tb.addField(c.name(), mapType(c.type()))
            );
        });

        return sb.build();
    }

    private static SqlTypeName mapType(CatalogType t) {
        return switch (t) {
            case INT -> SqlTypeName.INTEGER;
            case LONG -> SqlTypeName.BIGINT;
            case FLOAT -> SqlTypeName.FLOAT;
            case DOUBLE -> SqlTypeName.DOUBLE;
            case BOOL -> SqlTypeName.BOOLEAN;
            case STRING -> SqlTypeName.VARCHAR;
            case DATE -> SqlTypeName.DATE;
            default -> SqlTypeName.VARCHAR;
        };
    }
}
