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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.System.Logger.Level.INFO;

public final class OlapGatewayService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final System.Logger LOGGER = System.getLogger(OlapGatewayService.class.getName());

    private final CoordinatorClient coordinatorClient;
    private final AtomicLong currentSnapshotId;
    private final VmsGatewayClient gatewayClient;

    private final AtomicReference<CoordinatorCatalog> catalogRef = new AtomicReference<>(null);
    private volatile Orchestrator orchestrator = null;

    // ── QPO-1: physical plan cache ────────────────────────────────────────────
    // Key: SQL string. Value: compiled RelNode physical plan (template).
    // Cached after the first call — planning cost paid once.
    // Each execution gets a DEEP COPY of the cached plan via deepCopy()
    // to prevent concurrent threads from mutating each other's plan tree.
    // RelNode.copy() is shallow (inputs list is shared) — deepCopy()
    // recurses through the full tree, giving each thread a fully independent
    // copy safe for concurrent traversal by DistributedPlanner.
    private final ConcurrentHashMap<String, RelNode> planCache = new ConcurrentHashMap<>();

    public OlapGatewayService(CoordinatorClient coordinatorClient,
                              AtomicLong currentSnapshotId,
                              VmsGatewayClient gatewayClient) {
        this.coordinatorClient = coordinatorClient;
        this.currentSnapshotId = currentSnapshotId;
        this.gatewayClient     = gatewayClient;
        LOGGER.log(INFO, "OlapGatewayService created. Catalog will be fetched on first query.");
    }

    private CoordinatorCatalog getCatalog() {
        CoordinatorCatalog existing = catalogRef.get();
        if (existing != null) return existing;

        LOGGER.log(INFO, "Fetching catalog from coordinator (first query)...");
        CatalogResponse dtoCatalog = coordinatorClient.getCatalog();
        CoordinatorCatalog fresh = CatalogAdapter.toCoordinatorCatalog(dtoCatalog);
        LOGGER.log(INFO, "Catalog loaded. Schemas: " + fresh.getSchemaNames());

        if (catalogRef.compareAndSet(null, fresh)) {
            buildOrchestrator(fresh);
            return fresh;
        }
        return catalogRef.get();
    }

    private synchronized void buildOrchestrator(CoordinatorCatalog catalog) {
        if (orchestrator != null) return;
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

    // ── Getters for direct scan paths (QPO-2) ─────────────────────────────────

    public long getCurrentSnapshotId() {
        return currentSnapshotId.get();
    }

    public VmsGatewayClient getGatewayClient() {
        return gatewayClient;
    }

    public List<CatalogColumn> getColumnMetas(String schema, String table) {
        return resolveColumnMetas(schema, table, getCatalog());
    }

    // ── Query execution ───────────────────────────────────────────────────────

    public String execute(String sql) {
        long snapshot = currentSnapshotId.get();
        LOGGER.log(INFO, "OLAP GATEWAY SERVICE STARTING EXECUTION Snapshot: " + snapshot);

        CoordinatorCatalog catalog = getCatalog();

        // QPO-1: plan once, reuse the cached template on all subsequent calls.
        RelNode physical = planCache.computeIfAbsent(sql, s -> {
            LOGGER.log(INFO, "QPO-1: First call — planning SQL: "
                    + s.trim().substring(0, Math.min(80, s.trim().length())));
            QueryPlanner queryPlanner = new QueryPlanner(
                    catalog, new CalciteSchemaBuilder(), new CalcitePlannerImpl());
            var out = queryPlanner.plan(s, List.of(1, 100));
            LOGGER.log(INFO, "QPO-1: Plan cached. Cache size now: " + (planCache.size() + 1));
            return (RelNode) out.vmodbPhysicalPlan();
        });

        // QPO-1 thread safety: deep-copy the cached plan before execution.
        // RelNode.copy() is shallow — inputs are shared across copies.
        // DistributedPlanner traverses and mutates the full tree, so two
        // concurrent threads on the same tree corrupt each other's state.
        // deepCopy() recurses through all inputs, giving each thread a fully
        // independent tree. Cost: proportional to plan depth (~5 nodes for
        // CHQ4), negligible compared to TCP scan time.
        RelNode planCopy = deepCopy(physical);
        PushdownResponse result = orchestrator.execute(planCopy, snapshot);

        List<String> columns = physical.getRowType().getFieldNames();
        List<List<Object>> rows = result.rows == null ? List.of() : result.rows;
        List<LinkedHashMap<String, Object>> resultObjects = rowsAsObjects(columns, rows);

        return "{"
                + "\"resultColumns\":" + jsonValue(columns) + ","
                + "\"resultRowCount\":" + rows.size() + ","
                + "\"result\":" + jsonValue(resultObjects)
                + "}";
    }

    // ── QPO-1: deep copy helper ───────────────────────────────────────────────
    //
    // Recursively copies a RelNode tree so each execution thread gets a fully
    // independent copy. RelNode.copy(traitSet, inputs) is the standard Calcite
    // API for structural copying — every RelNode subclass is required to
    // implement it. We recurse through inputs first (bottom-up) so that each
    // copied parent receives already-copied children — no shared references.

    private static RelNode deepCopy(RelNode node) {
        List<RelNode> copiedInputs = node.getInputs().stream()
                .map(OlapGatewayService::deepCopy)
                .collect(Collectors.toList());
        return node.copy(node.getTraitSet(), copiedInputs);
    }

    // ── Catalog helpers ───────────────────────────────────────────────────────

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

    private static List<LinkedHashMap<String, Object>> rowsAsObjects(List<String> columns,
                                                                     List<List<Object>> rows) {
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