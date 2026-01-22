package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.calcite;

import dk.ku.di.dms.vms.calcite.schema.vModbSchema;
import dk.ku.di.dms.vms.calcite.schema.vModbTable;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CoordinatorCatalog;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

import static java.lang.System.Logger.Level.INFO;

public final class CalciteSchemaBuilder {

    private static final System.Logger LOGGER =
            System.getLogger(CalciteSchemaBuilder.class.getName());

    public SchemaPlus buildRootSchema(CoordinatorCatalog catalog) {
        SchemaPlus root = Frameworks.createRootSchema(false);

        catalog.schemaNames().forEach(schemaName -> {
            vModbSchema schema = buildOneSchema(catalog, schemaName);
            root.add(schemaName, schema);
        });

        return root;
    }

    private vModbSchema buildOneSchema(CoordinatorCatalog catalog, String schemaName) {
        vModbSchema.Builder schemaBuilder = vModbSchema.newBuilder(schemaName);

        catalog.tablesInSchema(schemaName).values().forEach(table -> {
            vModbTable.Builder tableBuilder = vModbTable.newBuilder(table.tableName());

            table.columns().forEach(column ->
                    tableBuilder.addField(column.name(), mapType(column.type()))
            );

            tableBuilder.withRowCount(1_000_000L);

            schemaBuilder.addTable(tableBuilder.build());
        });

        return schemaBuilder.build();
    }

    private SqlTypeName mapType(CatalogType catalogType) {
        return switch (catalogType) {
            case INT -> SqlTypeName.INTEGER;
            case BIGINT -> SqlTypeName.BIGINT;
            case FLOAT -> SqlTypeName.FLOAT;
            case DOUBLE -> SqlTypeName.DOUBLE;
            case BOOLEAN -> SqlTypeName.BOOLEAN;
            case VARCHAR -> SqlTypeName.VARCHAR;
            case DATE -> SqlTypeName.DATE;
            case TIMESTAMP -> SqlTypeName.TIMESTAMP;
            case BYTES -> SqlTypeName.VARBINARY;
            case LONG -> null;
            case BOOL -> null;
            case STRING -> null;
        };
    }
}