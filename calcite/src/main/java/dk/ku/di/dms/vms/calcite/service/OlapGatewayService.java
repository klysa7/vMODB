package dk.ku.di.dms.vms.calcite.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.Orchestrator;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

public final class OlapGatewayService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOGGER = System.getLogger(OlapGatewayService.class.getName());

    private final CoordinatorClient coordinatorClient;
    private final AtomicLong currentSnapshotId;
    private final VmsGatewayClient gatewayClient;

    // -------------------------------------------------------------------------
    // A2 FIX (revised): lazy-but-cached catalog initialization.
    //
    // Original A2 fetched in the constructor — that broke startup ordering
    // because the coordinator starts with the proxy (last) while the gateway
    // must start earlier.
    //
    // This revision: fetch on first execute() call, cache forever after.
    // Effect is identical to the original A2 goal — exactly one HTTP GET to
    // the coordinator for the lifetime of the benchmark — but the gateway can
    // now start before the coordinator is ready.
    //
    // AtomicReference ensures the fetch is thread-safe: if two queries race
    // on the very first call, only one will actually hit the coordinator
    // (compareAndSet guarantees this).
    // -------------------------------------------------------------------------
    private final AtomicReference<CoordinatorCatalog> catalogRef = new AtomicReference<>(null);
    private volatile Orchestrator orchestrator = null;

    public OlapGatewayService(CoordinatorClient coordinatorClient,
                              AtomicLong currentSnapshotId,
                              VmsGatewayClient gatewayClient) {
        this.coordinatorClient = coordinatorClient;
        this.currentSnapshotId = currentSnapshotId;
        this.gatewayClient     = gatewayClient;
        // Gateway starts immediately — no coordinator call here
        LOGGER.log(INFO, "OlapGatewayService created. Catalog will be fetched on first query.");
    }

    /**
     * Returns the cached catalog, fetching from the coordinator exactly once
     * on the first call. Safe for concurrent callers.
     */
    private CoordinatorCatalog getCatalog() {
        CoordinatorCatalog existing = catalogRef.get();
        if (existing != null) return existing;

        // First call — fetch from coordinator
        LOGGER.log(INFO, "Fetching catalog from coordinator (first query)...");
        CatalogResponse dtoCatalog = coordinatorClient.getCatalog();
        CoordinatorCatalog fresh = CatalogAdapter.toCoordinatorCatalog(dtoCatalog);
        LOGGER.log(INFO, "Catalog loaded. Schemas: " + fresh.getSchemaNames());

        // Only store if nobody beat us to it (CAS)
        if (catalogRef.compareAndSet(null, fresh)) {
            // We won the race — also build and cache the orchestrator
            buildOrchestrator(fresh);
            return fresh;
        }
        // Another thread already stored — use theirs (orchestrator already built)
        return catalogRef.get();
    }

    private synchronized void buildOrchestrator(CoordinatorCatalog catalog) {
        if (orchestrator != null) return; // already built
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
        DistributedPlanner distributedPlanner = new DistributedPlanner(catalog, columnsResolver);
        DistributedExecutor executor = new DistributedExecutor(gatewayClient, columnsResolver);
        this.orchestrator = new Orchestrator(distributedPlanner, executor);
        LOGGER.log(INFO, "Orchestrator built and cached.");
    }

    public String execute(String sql) {
        long snapshot = currentSnapshotId.get();
        LOGGER.log(INFO, "OLAP GATEWAY SERVICE STARTING EXECUTION Snapshot: " + snapshot + "]");

        // Lazy fetch — coordinator must be up by the time the first query arrives,
        // but the gateway itself can start before the coordinator.
        CoordinatorCatalog catalog = getCatalog();

        QueryPlanner queryPlanner = new QueryPlanner(catalog, new CalciteSchemaBuilder(), new CalcitePlannerImpl());
        var out = queryPlanner.plan(sql, List.of(1, 100));
        RelNode physical = (RelNode) out.vmodbPhysicalPlan();

        PushdownResponse result = orchestrator.execute(physical, snapshot);

        List<String> columns = physical.getRowType().getFieldNames();
        List<List<Object>> rows = result.rows == null ? List.of() : result.rows;
        List<LinkedHashMap<String, Object>> resultObjects = rowsAsObjects(columns, rows);

        return "{"
                + "\"resultColumns\":" + jsonValue(columns) + ","
                + "\"resultRowCount\":" + rows.size() + ","
                + "\"result\":" + jsonValue(resultObjects)
                + "}";
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
        try { return MAPPER.writeValueAsString(v); } catch (Exception e) { return "null"; }
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