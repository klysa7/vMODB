package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.client.VmsGatewayClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalJoinOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.LocalProjectOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.StreamingScanOperator;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER = System.getLogger(DistributedExecutor.class.getName());

    private final VmsGatewayClient gatewayClient;

    public DistributedExecutor(VmsGatewayClient gatewayClient) {
        this.gatewayClient = gatewayClient;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long startNano = System.nanoTime();

        LOGGER.log(INFO, ">>> [EXECUTOR] Starting Execution for Snapshot #" + distributedPlan.snapshot);

        CoordinatorOperator root = buildOperatorTree(distributedPlan.root, distributedPlan);

        LOGGER.log(INFO, ">>> [EXECUTOR] Opening Pipeline...");
        root.open();

        List<List<Object>> allRows = new ArrayList<>();
        List<Object[]> batch;
        long totalRows = 0;

        LOGGER.log(INFO, ">>> [EXECUTOR] Entering Fetch Loop...");
        try {
            while ((batch = root.nextBatch()) != null) {
                totalRows += batch.size();
                for (Object[] row : batch) {
                    allRows.add(Arrays.asList(row));
                }
            }
        } catch (Exception e) {
            LOGGER.log(ERROR, "Error during execution", e);
            throw e;
        } finally {
            LOGGER.log(INFO, ">>> [EXECUTOR] Closing Pipeline");
            root.close();
        }

        long endNano = System.nanoTime();
        double durationMs = (endNano - startNano) / 1_000_000.0;
        double throughput = (durationMs > 0) ? (totalRows / (durationMs / 1000.0)) : 0.0;

        System.out.println("========================================================");
        System.out.println(String.format("   Total Rows Returned : %d", totalRows));
        System.out.println(String.format("   Total Latency (ms)  : %.3f ms", durationMs));
        System.out.println(String.format("   Throughput (rows/s) : %.2f rows/sec", throughput));
        System.out.println("========================================================\n");

        return new PushdownResponse("gateway", distributedPlan.snapshot, null, allRows);
    }

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition def, DistributedPlan plan) {

        if (def instanceof ScanDefinition scanDef) {
            VmsSubplan subplan = plan.subPlans.stream()
                    .filter(s -> s.exchangeId.equals(scanDef.exchangeId()))
                    .findFirst()
                    .orElseThrow();
            return new StreamingScanOperator(gatewayClient, subplan, plan.snapshot);
        }

        if (def instanceof JoinDefinition joinDef) {
            if (joinDef.left() instanceof ScanDefinition leftScan
                    && joinDef.right() instanceof ScanDefinition rightScan) {

                VmsSubplan leftPlan  = findSubplan(plan, leftScan.exchangeId());
                VmsSubplan rightPlan = findSubplan(plan, rightScan.exchangeId());

                String leftTable  = ((ScanAllOperation) leftPlan.operation).table;
                String rightTable = ((ScanAllOperation) rightPlan.operation).table;
                String leftSchema = ((ScanAllOperation) leftPlan.operation).schema;
                String rightSchema= ((ScanAllOperation) rightPlan.operation).schema;

                int leftPort  = extractPort(leftPlan.url);
                int rightPort = extractPort(rightPlan.url);

                // ── Build JoinRoutingData for the ORDER (probe) side ──────────────────
                // remote = customer (left/build side); local = orders (right/probe side)
                List<CatalogColumn> remoteColumns =
                        columnsResolver.columnMetas(leftSchema, leftTable);
                List<CatalogColumn> localColumns  =
                        columnsResolver.columnMetas(rightSchema, rightTable);

                int numJoinCols = joinDef.leftKeys().length;
                int[] remoteOffsets  = new int[numJoinCols];
                byte[] remoteTypes   = new byte[numJoinCols];
                int[] localIndices   = new int[numJoinCols];

                for (int i = 0; i < numJoinCols; i++) {
                    int remoteColIdx = joinDef.leftKeys()[i];
                    remoteOffsets[i] = byteOffsetOf(remoteColumns, remoteColIdx);
                    remoteTypes[i]   = typeCode(remoteColumns.get(remoteColIdx).type());
                    localIndices[i]  = joinDef.rightKeys()[i];
                }
                int remoteRecordSize = totalByteSize(remoteColumns);

                JoinRoutingData routing = new JoinRoutingData(
                        remoteOffsets, remoteTypes, localIndices, remoteRecordSize);

                // ── Trigger broadcast from Warehouse → Order ───────────────────────────
                final String targetAddr = extractHost(leftPlan.url) + ":" + rightPort;
                scheduleWithDelay(() -> gatewayClient.triggerBroadcast(
                        extractHost(rightPlan.url), leftPort,
                        plan.snapshot, plan.snapshot,
                        leftTable, leftPlan.predicates, targetAddr), 100);

                // ── Build combined column list for result deserialization ──────────────
                List<CatalogColumn> combined = new ArrayList<>(remoteColumns);
                combined.addAll(localColumns);

                VmsSubplan receiverPlan = new VmsSubplan(
                        rightPlan.vmsName, rightPlan.url, rightPlan.exchangeId,
                        rightPlan.operation, toNames(combined),
                        rightPlan.predicates, (byte) 2, routing.toBytes()
                );
                receiverPlan.setColumnLayouts(buildLayouts(combined)); // see below

                return new StreamingScanOperator(gatewayClient, receiverPlan, plan.snapshot);
            }

        if (def instanceof ProjectDefinition projDef) {
            return new LocalProjectOperator(buildOperatorTree(projDef.input(), plan), projDef.projectedIndices());
        }

        throw new IllegalArgumentException("Unknown Op: " + def);
    }

    private int resolveTpccPort(String tableName) {
        tableName = tableName.toLowerCase();
        if (tableName.contains("warehouse") || tableName.contains("customer")) return 8001;
        if (tableName.contains("item") || tableName.contains("stock")) return 8002;
        return 8003;
    }

        private static int byteOffsetOf(List<CatalogColumn> cols, int colIndex) {
            int offset = 0;
            for (int i = 0; i < colIndex; i++) {
                offset += cols.get(i).byteSize();
            }
            return offset;
        }

        private static int totalByteSize(List<CatalogColumn> cols) {
            return cols.stream().mapToInt(CatalogColumn::byteSize).sum();
        }

        private static byte typeCode(CatalogType t) {
            return switch (t) {
                case BIGINT, LONG -> JoinRoutingData.TYPE_LONG;
                case DOUBLE -> JoinRoutingData.TYPE_DOUBLE;
                default -> JoinRoutingData.TYPE_INT;
            };
        }

        private static int extractPort(String url) {
            // url is like "http://localhost:8001/table"
            return URI.create(url.replace("http://","")).getPort();
        }

        private static String extractHost(String url) {
            return URI.create(url.replace("http://","")).getHost();
        }
}