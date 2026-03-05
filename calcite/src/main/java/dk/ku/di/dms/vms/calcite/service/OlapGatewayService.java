package dk.ku.di.dms.vms.calcite.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlanner;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.DistributedExecutor;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.QueryPlanner;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlannerImpl;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static java.lang.System.Logger.Level.INFO;

public final class OlapGatewayService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOGGER = System.getLogger(OlapGatewayService.class.getName());
    private static final Map<String, Integer> TPCC_PORTS = new HashMap<>();
    static {
        TPCC_PORTS.put("warehouse", 8001);
        TPCC_PORTS.put("inventory", 8002);
        TPCC_PORTS.put("order", 8003);
    }

    private final CoordinatorClient coordinatorClient;
    private final AtomicLong currentSnapshotId;
    private final VmsGatewayClient gatewayClient;

    public OlapGatewayService(CoordinatorClient coordinatorClient,
                              AtomicLong currentSnapshotId,
                              VmsGatewayClient gatewayClient) {
        this.coordinatorClient = coordinatorClient;
        this.currentSnapshotId = currentSnapshotId;
        this.gatewayClient = gatewayClient;
    }

    public String execute(String sql) {
        long snapshot = currentSnapshotId.get();
        LOGGER.log(INFO, "OLAP GATEWAY SERVICE STARTING EXECUTION Snapshot: " + snapshot + "]");

        CatalogResponse dtoCatalog = coordinatorClient.getCatalog();
        CoordinatorCatalog catalog = CatalogAdapter.toCoordinatorCatalog(dtoCatalog);
        LOGGER.log(INFO, "CATALOG CONTENT: " + catalog.getSchemaNames());

        QueryPlanner queryPlanner = new QueryPlanner(catalog, new CalciteSchemaBuilder(), new CalcitePlannerImpl());
        var out = queryPlanner.plan(sql, List.of(1, 100));
        RelNode physical = (RelNode) out.vmodbPhysicalPlan();

        PlacementResolver placement = new PlacementResolver();
        DistributedPlanner.ColumnsResolver columnsResolver = new DistributedPlanner.ColumnsResolver() {
            @Override
            public List<String> columnsInOrder(String schema, String table) {
                return resolveColumnNames(schema, table, catalog);
            }

            @Override
            public List<CatalogColumn> columnMetas(String schema, String table) {
                return resolveColumnMetas(schema, table, catalog);
            }
        };

        DistributedPlanner distributedPlanner = new DistributedPlanner(placement, columnsResolver);
        DistributedExecutor executor = new DistributedExecutor(this.gatewayClient, columnsResolver);

        Orchestrator orchestrator = new Orchestrator(distributedPlanner, executor);
        PushdownResponse result = orchestrator.execute(physical, snapshot);

        List<String> columns = physical.getRowType().getFieldNames();
        List<List<Object>> rows = result.rows == null ? List.of() : result.rows;
        List<LinkedHashMap<String, Object>> resultObjects = rowsAsObjects(columns, rows);

        String jsonOutput = "{"
                + "\"resultColumns\":" + jsonValue(columns) + ","
                + "\"resultRowCount\":" + rows.size() + ","
                + "\"result\":" + jsonValue(resultObjects)
                + "}";
        return jsonOutput;
    }

    private List<String> resolveColumnNames(String schema, String table, CoordinatorCatalog catalog) {
        var schemaObj = catalog.tablesInSchema(schema);
        if (schemaObj == null) throw new IllegalArgumentException("Schema not found: " + schema);
        var t = schemaObj.get(table);
        if (t == null) throw new IllegalArgumentException("Table not found: " + schema + "." + table);
        return t.columns().stream().map(CatalogColumn::name).toList();
    }

    private List<CatalogColumn> resolveColumnMetas(String schema, String table, CoordinatorCatalog catalog) {
        var schemaObj = catalog.tablesInSchema(schema);
        if (schemaObj == null) throw new IllegalArgumentException("Schema not found: " + schema);
        var t = schemaObj.get(table);
        if (t == null) throw new IllegalArgumentException("Table not found: " + schema + "." + table);
        return t.columns();
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
            var obj = new LinkedHashMap<String, Object>();
            IntStream.range(0, columns.size()).forEach(i -> {
                Object val = (row != null && i < row.size()) ? row.get(i) : null;
                obj.put(columns.get(i), val);
            });
            return obj;
        }).toList();
    }
}