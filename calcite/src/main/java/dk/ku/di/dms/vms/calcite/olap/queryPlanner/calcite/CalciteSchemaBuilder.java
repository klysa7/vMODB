package dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.schema.vModbSchema;
import dk.ku.di.dms.vms.calcite.schema.vModbTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;

import java.util.Map;

public final class CalciteSchemaBuilder {

    private static final System.Logger LOGGER =
            System.getLogger(CalciteSchemaBuilder.class.getName());

    // B30 FIX: TPC-C heuristic row counts per table name.
    //
    // BEFORE: tableBuilder.withRowCount(1_000_000L) for every table.
    //   The VolcanoPlanner uses row counts for join cost estimation and
    //   operator ordering. With every table at 1M rows, the planner cannot
    //   exploit size differences — it treats a 1-row warehouse table as
    //   equally expensive to scan as a 300K-row order_line table.
    //   For CHQ3 (4-table join), wrong cardinalities lead to suboptimal
    //   join ordering: the planner may probe a large table before a small
    //   one, increasing intermediate result set sizes.
    //
    // AFTER: known TPC-C row counts scaled by num_ware=1.
    //   The coordinator does not send row counts in its catalog response
    //   (CoordinatorCatalogBuilder builds from VmsDataModel which has no
    //   rowCount field). A protocol change to propagate actual counts is
    //   future work. As an intermediate fix, we use TPC-C specification
    //   row counts for known table names. Unknown tables fall back to
    //   the previous default of 1,000,000.
    //
    // TPC-C row counts (num_ware=1, standard scale factor):
    //   order_line  : ~300,000  (avg 10 lines × 3000 orders × 10 districts)
    //   orders      :   30,000  (3000 × 10 districts)
    //   new_orders  :    9,000  (~900 × 10 districts, newest 900 orders)
    //   customer    :   30,000  (3000 × 10 districts)
    //   history     :   30,000  (1 per customer initially)
    //   item        :  100,000  (fixed, not scaled by num_ware)
    //   stock       :  100,000  (100K × num_ware)
    //   district    :       10  (10 per warehouse)
    //   warehouse   :        1  (= num_ware)
    //
    // Impact: most visible for CHQ3 (4-table join: customer, orders,
    //   new_orders, order_line) where the planner can now correctly identify
    //   new_orders (9K) as the smallest probe table.
    //
    // Cite: Ioannidis 1996 "Query Optimization" (ACM Computing Surveys)
    //   — cardinality estimation as the fundamental driver of join ordering.
    //   Future work: coordinator sends actual row counts via catalog response,
    //   eliminating the need for this static map.
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