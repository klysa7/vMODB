package dk.ku.di.dms.vms.calcite.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.DistributedExecutor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.QueryPlanner;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlannerImpl;
import org.apache.calcite.rel.RelNode;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.IntStream;


public final class OlapGatewayService {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final System.Logger LOGGER =
            System.getLogger(OlapGatewayService.class.getName());

    private final CoordinatorClient coordinatorClient;

    public OlapGatewayService(CoordinatorClient coordinatorClient) {
        this.coordinatorClient = coordinatorClient;
    }

    public String execute(String sql) {

        var snap = coordinatorClient.getSnapshot();
        var dtoCatalog = coordinatorClient.getCatalog();

        Long snapshotId = snap.snapshotId();
        CoordinatorCatalog catalog = CatalogAdapter.toCoordinatorCatalog(dtoCatalog);

        QueryPlanner queryPlanner = new QueryPlanner(catalog, new CalciteSchemaBuilder(),
                new CalcitePlannerImpl());

        var out = queryPlanner.plan(sql, List.of(1, 100));
        RelNode physical = (RelNode) out.vmodbPhysicalPlan();

        PlacementResolver placement = new PlacementResolver();

        DistributedPlanner.ColumnsResolver columnsResolver =
                (schema, table) -> resolveColumns(schema, table, catalog);

        DistributedPlanner distPlanner = new DistributedPlanner(placement, columnsResolver);
        DistributedExecutor executor = new DistributedExecutor(new VmsHttpClient());
        Orchestrator orchestrator = new Orchestrator(distPlanner, executor);

        PushdownResponse result = orchestrator.execute(physical, snapshotId);

        List<String> columns = result.columns == null ? List.of() : result.columns;
        List<List<Object>> rows = result.rows == null ? List.of() : result.rows;
        List<LinkedHashMap<String, Object>> resultObjects = rowsAsObjects(columns, rows);

        return "{"
                + "\"resultColumns\":" + jsonValue(columns) + ","
                + "\"resultRowCount\":" + rows.size() + ","
                + "\"result\":" + jsonValue(resultObjects)
                + "}";
    }

    private List<String> resolveColumns(String schema, String table, CoordinatorCatalog catalog) {
        var t = catalog.tablesInSchema(schema).get(table);

        if (t == null) {
            throw new IllegalArgumentException(
                    "Table does not exist in catalog " + schema + "." + table
            );
        }
        return t.columns().stream()
                .map(c -> c.name())
                .toList();
    }

    private static String jsonValue(Object v) {
        try {
            return MAPPER.writeValueAsString(v);
        } catch (Exception e) {
            return "null";
        }
    }

    private static List<LinkedHashMap<String, Object>> rowsAsObjects(List<String> columns, List<List<Object>> rows
    ) {
        return rows.stream()
                .map(row -> {
                    var obj = new java.util.LinkedHashMap<String, Object>();
                    IntStream.range(0, columns.size()).forEach(i -> {
                        Object val = (row != null && i < row.size()) ? row.get(i) : null;
                        obj.put(columns.get(i), val);
                    });
                    return obj;
                })
                .toList();
    }
}