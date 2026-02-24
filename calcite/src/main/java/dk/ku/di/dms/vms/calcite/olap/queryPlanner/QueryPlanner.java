package dk.ku.di.dms.vms.calcite.olap.queryPlanner;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlanner;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;

public final class QueryPlanner {

    private final CoordinatorCatalog catalog;
    private final CalciteSchemaBuilder schemaBuilder;
    private final CalcitePlanner calcitePlanner;

    public QueryPlanner(CoordinatorCatalog catalog, CalciteSchemaBuilder schemaBuilder,
                        CalcitePlanner calcitePlanner) {
        this.catalog = catalog;
        this.schemaBuilder = schemaBuilder;
        this.calcitePlanner = calcitePlanner;
    }

    public CalcitePlanner.PlanOutput plan(String sql, List<Object> params) {
        List<Object> safeParams = (params == null) ? List.of() : params;

        SchemaPlus root = buildRootSchema();
        return calcitePlanner.planJoinOnly(sql, root, safeParams);
    }

    private SchemaPlus buildRootSchema() {
        return schemaBuilder.buildRootSchema(catalog);
    }
}