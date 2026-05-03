package dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.schema.vModbSchema;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.schema.vModbTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public final class CalciteSchemaBuilder {

    private static final System.Logger LOGGER =
            System.getLogger(CalciteSchemaBuilder.class.getName());

    private static final Map<String, Long> TPCC_ROW_COUNTS = Map.of(
            "order_line",  300_000L,
            "orders",       30_000L,
            "new_orders",    9_000L,
            "customer",     30_000L,
            "history",      30_000L,
            "item",        100_000L,
            "stock",       100_000L,
            "district",         10L,
            "warehouse",         1L
    );

    public SchemaPlus buildRootSchema(CoordinatorCatalog catalog) {
        SchemaPlus root = Frameworks.createRootSchema(false);

        catalog.getSchemaNames().forEach(schemaName -> {
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

            // B30 FIX: use TPC-C heuristic row count instead of 1,000,000.
            // Falls back to 1,000,000 for any table not in the known map.
            long rowCount = TPCC_ROW_COUNTS.getOrDefault(table.tableName(), 1_000_000L);
            tableBuilder.withRowCount(rowCount);

            LOGGER.log(System.Logger.Level.DEBUG,
                    "B30: table=" + table.tableName() + " rowCount=" + rowCount);

            schemaBuilder.addTable(tableBuilder.build());
        });

        return schemaBuilder.build();
    }

    private static SqlTypeName mapType(CatalogType catalogType) {
        return switch (catalogType) {
            case INT            -> SqlTypeName.INTEGER;
            case BIGINT, LONG   -> SqlTypeName.BIGINT;
            case FLOAT          -> SqlTypeName.FLOAT;
            case DOUBLE         -> SqlTypeName.DOUBLE;
            case BOOLEAN, BOOL  -> SqlTypeName.BOOLEAN;
            case VARCHAR, STRING -> SqlTypeName.VARCHAR;
            case DATE           -> SqlTypeName.DATE;
            case TIMESTAMP      -> SqlTypeName.TIMESTAMP;
            case BYTES          -> SqlTypeName.VARBINARY;
        };
    }
}