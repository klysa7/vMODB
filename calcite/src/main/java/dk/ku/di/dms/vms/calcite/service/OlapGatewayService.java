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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.lang.System.Logger.Level.INFO;

public final class OlapGatewayService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOGGER =
            System.getLogger(OlapGatewayService.class.getName());

    private final CoordinatorClient coordinatorClient;
    private final AtomicLong currentSnapshotId;

    public OlapGatewayService(CoordinatorClient coordinatorClient, AtomicLong currentSnapshotId) {
        this.coordinatorClient = coordinatorClient;
        this.currentSnapshotId = currentSnapshotId;
    }

    public String execute(String sql) {
        //TODO finalize that Coordinator returns a snapshot, maybe do the 2 calls 1 or find wa way to minimize the transfer
//        var snap = coordinatorClient.getSnapshot();
//        Long snapshotId = (snap.snapshotId() > 0) ? snap.snapshotId() : 1L;

        LOGGER.log(INFO, "OLAP GATEWAY SERVICE STARTING EXECUTION Snapshot: " + currentSnapshotId.get() + "]");

        var dtoCatalog = coordinatorClient.getCatalog();
        CoordinatorCatalog catalog = CatalogAdapter.toCoordinatorCatalog(dtoCatalog);

        //the first part is the Query Planning
        //TODO more details in the doc, but apply logic to plan every query and not only Project(Join(Scan))
        QueryPlanner queryPlanner = new QueryPlanner(catalog,
                new CalciteSchemaBuilder(), new CalcitePlannerImpl());
        var out = queryPlanner.plan(sql, List.of(1, 100));
        RelNode physical = (RelNode) out.vmodbPhysicalPlan();

        //orechestrator and execution
        PlacementResolver placement = new PlacementResolver();
        DistributedPlanner.ColumnsResolver columnsResolver =
                (schema, table) -> resolveColumns(schema, table, catalog);

        DistributedPlanner distributedPlanner = new DistributedPlanner(placement, columnsResolver);
        DistributedExecutor executor = new DistributedExecutor(new VmsHttpClient());
        Orchestrator orchestrator = new Orchestrator(distributedPlanner, executor);


        PushdownResponse result = orchestrator.execute(physical, currentSnapshotId.get());
        List<String> columns = physical.getRowType().getFieldNames();
        List<List<Object>> rows = result.rows == null ? List.of() : result.rows;
        List<LinkedHashMap<String, Object>> resultObjects = rowsAsObjects(columns, rows);

        String jsonOutput = "{"
                + "\"resultColumns\":" + jsonValue(columns) + ","
                + "\"resultRowCount\":" + rows.size() + ","
                + "\"result\":" + jsonValue(resultObjects)
                + "}";

        LOGGER.log(INFO, "Result JSON Size: " + jsonOutput.length() + " chars");
        return jsonOutput;
    }

    private List<String> resolveColumns(String schema, String table, CoordinatorCatalog catalog) {
        var t = catalog.tablesInSchema(schema).get(table);
        if (t == null) throw new IllegalArgumentException("Table not found: " + schema + "." + table);
        return t.columns().stream().map(c -> c.name()).toList();
    }

    private static String jsonValue(Object v) {
        try {
            return MAPPER.writeValueAsString(v);
        } catch (Exception e) {
            return "null";
        }
    }

    private static List<LinkedHashMap<String, Object>> rowsAsObjects(List<String> columns, List<List<Object>> rows) {
        return rows.stream().map(row -> {
            var obj = new java.util.LinkedHashMap<String, Object>();
            IntStream.range(0, columns.size()).forEach(i -> {
                Object val = (row != null && i < row.size()) ? row.get(i) : null;
                obj.put(columns.get(i), val);
            });
            return obj;
        }).toList();
    }
}