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
import java.util.concurrent.TimeUnit;
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
    private final ConcurrentHashMap<String, RelNode> planCache = new ConcurrentHashMap<>();

    private static final class PlanStats {
        final AtomicLong planOnceNs    = new AtomicLong(0);
        final AtomicLong hitCount      = new AtomicLong(0);
        final AtomicLong lookupTotalNs = new AtomicLong(0);
        final AtomicLong copyTotalNs   = new AtomicLong(0);
    }
    private final ConcurrentHashMap<String, PlanStats> planStats = new ConcurrentHashMap<>();
    private final AtomicLong planCacheMisses = new AtomicLong(0);

    public OlapGatewayService(CoordinatorClient coordinatorClient,
                              AtomicLong currentSnapshotId,
                              VmsGatewayClient gatewayClient) {
        this.coordinatorClient = coordinatorClient;
        this.currentSnapshotId = currentSnapshotId;
        this.gatewayClient     = gatewayClient;
        LOGGER.log(INFO, "OlapGatewayService created. Catalog will be fetched on first query.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            String dump = dumpPlanCacheStats();
            if (!dump.isEmpty()) {
                System.err.println("\n===== QPO-1 plan cache summary =====");
                System.err.println(dump);
                System.err.println("====================================");
            }
        }, "qpo1-stats-dump"));
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

    public long getCurrentSnapshotId() {
        return currentSnapshotId.get();
    }

    public VmsGatewayClient getGatewayClient() {
        return gatewayClient;
    }

    public List<CatalogColumn> getColumnMetas(String schema, String table) {
        return resolveColumnMetas(schema, table, getCatalog());
    }

    public String execute(String sql) {
        long snapshot = currentSnapshotId.get();
        LOGGER.log(INFO, "OLAP GATEWAY SERVICE STARTING EXECUTION Snapshot: " + snapshot);

        CoordinatorCatalog catalog = getCatalog();
        PlanStats stats = planStats.computeIfAbsent(sql, k -> new PlanStats());

        long lookupStart = System.nanoTime();
        final boolean[] wasMiss = {false};
        RelNode physical = planCache.computeIfAbsent(sql, s -> {
            wasMiss[0] = true;
            long planStart = System.nanoTime();
            LOGGER.log(INFO, "QPO-1: First call — planning SQL: "
                    + s.trim().substring(0, Math.min(80, s.trim().length())));
            QueryPlanner queryPlanner = new QueryPlanner(
                    catalog, new CalciteSchemaBuilder(), new CalcitePlannerImpl());
            var out = queryPlanner.plan(s, List.of(1, 100));
            long planNs = System.nanoTime() - planStart;
            stats.planOnceNs.set(planNs);
            LOGGER.log(INFO, "QPO-1: first-plan took " + TimeUnit.NANOSECONDS.toMillis(planNs)
                    + " ms. Cache size now: " + (planCache.size() + 1));
            return (RelNode) out.vmodbPhysicalPlan();
        });
        long lookupNs = System.nanoTime() - lookupStart;

        if (wasMiss[0]) {
            planCacheMisses.incrementAndGet();
        } else {
            stats.hitCount.incrementAndGet();
            stats.lookupTotalNs.addAndGet(lookupNs);
        }

        long copyStart = System.nanoTime();
        RelNode planCopy = deepCopy(physical);
        stats.copyTotalNs.addAndGet(System.nanoTime() - copyStart);

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

    public String dumpPlanCacheStats() {
        if (planStats.isEmpty()) return "";
        long totalHits   = 0;
        long totalMisses = planCacheMisses.get();
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("hits_total=%d misses_total=%d distinct_queries=%d%n",
                totalMisses /* placeholder, set below */,
                totalMisses,
                planStats.size()));
        int hdrInsertAt = sb.length();

        for (var entry : planStats.entrySet()) {
            String sql = entry.getKey();
            PlanStats s = entry.getValue();
            long hits   = s.hitCount.get();
            totalHits  += hits;
            long calls  = hits + 1;
            long planNs = s.planOnceNs.get();
            long lookupAvgNs = hits > 0 ? s.lookupTotalNs.get() / hits : 0;
            long copyAvgNs   = s.copyTotalNs.get() / Math.max(1, calls);

            long savingPerHitNs = planNs - lookupAvgNs - copyAvgNs;
            long aggSavedMs     = TimeUnit.NANOSECONDS.toMillis(savingPerHitNs * hits);

            String preview = sql.trim().replace('\n', ' ').replaceAll("\\s+", " ");
            preview = preview.substring(0, Math.min(60, preview.length()));

            sb.append(String.format(
                    "[%-60s] hits=%-8d plan_once=%dms lookup_avg=%dus copy_avg=%dus "
                            + "saving_per_hit=%dms agg_saved=%dms%n",
                    preview, hits,
                    TimeUnit.NANOSECONDS.toMillis(planNs),
                    TimeUnit.NANOSECONDS.toMicros(lookupAvgNs),
                    TimeUnit.NANOSECONDS.toMicros(copyAvgNs),
                    TimeUnit.NANOSECONDS.toMillis(Math.max(0, savingPerHitNs)),
                    aggSavedMs));
        }

        String header = String.format("hits_total=%d misses_total=%d distinct_queries=%d%n",
                totalHits, totalMisses, planStats.size());
        sb.replace(0, hdrInsertAt, header);
        return sb.toString();
    }

    private static RelNode deepCopy(RelNode node) {
        List<RelNode> copiedInputs = node.getInputs().stream()
                .map(OlapGatewayService::deepCopy)
                .collect(Collectors.toList());
        return node.copy(node.getTraitSet(), copiedInputs);
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