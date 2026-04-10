package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionContext;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.definition.key.SimpleKey;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.Analyzer;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.exception.AnalyzerException;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContextBuilder;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexAggregateScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScan;
import dk.ku.di.dms.vms.modb.query.planner.SimplePlanner;
import dk.ku.di.dms.vms.modb.storage.iterator.IRecordIterator;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.NonUniqueSecondaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.PrimaryIndex;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.UniqueSecondaryIndex;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static dk.ku.di.dms.vms.modb.common.memory.MemoryUtils.UNSAFE;
import static dk.ku.di.dms.vms.modb.definition.Schema.RECORD_HEADER;
import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;

public final class TransactionManager implements OperationalAPI, ITransactionManager {

    private static final System.Logger LOGGER = System.getLogger(TransactionManager.class.getName());

    public record SimplePredicate(int columnPosition, ExpressionTypeEnum expression, Object value) {}

    private final Map<Long, TransactionContext> txCtxMap;
    private final Analyzer analyzer;
    private final SimplePlanner planner;
    private final Map<String, AbstractSimpleOperator> queryPlanCacheMap;
    public final Map<String, Table> catalog;

    public TransactionManager(Map<String, Table> catalog) {
        this.planner = new SimplePlanner();
        this.analyzer = new Analyzer(catalog);
        this.catalog = catalog;
        this.queryPlanCacheMap = new ConcurrentHashMap<>();
        this.txCtxMap = new ConcurrentHashMap<>(2048 * 10);
    }

    private boolean fkConstraintViolation(TransactionContext txCtx, Table table, Object[] values) {
        for (Map.Entry<PrimaryIndex, int[]> entry : table.foreignKeys().entrySet()) {
            IKey fk = KeyUtils.buildRecordKey(entry.getValue(), values);
            return !entry.getKey().exists(txCtx, fk);
        }
        return false;
    }

    @Override
    public List<Object[]> fetch(final Table table, final SelectStatement selectStatement) {
        String sqlAsKey = selectStatement.SQL.toString();
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.computeIfAbsent(sqlAsKey, (_) -> {
            QueryTree queryTree = this.analyzer.analyze(selectStatement);
            return this.planner.plan(queryTree);
        });
        List<WherePredicate> wherePredicates;
        if (!selectStatement.whereClause.isEmpty()) {
            wherePredicates = this.analyzer.analyzeWhere(table, selectStatement.whereClause);
        } else {
            wherePredicates = Collections.emptyList();
        }
        if (scanOperator.isIndexScan()) {
            if (wherePredicates.get(0).expression == ExpressionTypeEnum.EQUALS) {
                IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, scanOperator.asIndexScan().index());
                if (wherePredicates.size() == key.size()) {
                    return scanOperator.asIndexScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
                } else {
                    List<WherePredicate> nonIdxClause = this.getNonIndexedColumnsWhereClause(wherePredicates, scanOperator.asIndexScan().index());
                    FilterContext filterContext = FilterContextBuilder.build(nonIdxClause);
                    return scanOperator.asIndexScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, filterContext);
                }
            } else {
                IKey[] keys = this.getMultiKeysFromWhereClause(wherePredicates.get(0));
                return scanOperator.asIndexScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), keys);
            }
        } else if (scanOperator.isIndexScanWithOrder()) {
            IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, scanOperator.asIndexScanWithOrder().index());
            if (wherePredicates.size() == key.size()) {
                return scanOperator.asIndexScanWithOrder().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
            } else {
                List<WherePredicate> nonIdxClause = this.getNonIndexedColumnsWhereClause(wherePredicates, scanOperator.asIndexScanWithOrder().index());
                FilterContext filterContext = FilterContextBuilder.build(nonIdxClause);
                return scanOperator.asIndexScanWithOrder().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key, filterContext);
            }
        } else if (scanOperator.isFullScanWithOrder()) {
            FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
            return scanOperator.asFullScanWithOrder().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), filterContext);
        } else if (scanOperator.isIndexAggregationScan()) {
            return scanOperator.asIndexAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()));
        } else if (scanOperator.isIndexMultiAggregationScan()) {
            IKey key = this.getIndexedKeysFromWhereClause(wherePredicates, scanOperator.asIndexMultiAggregationScan().index());
            return scanOperator.asIndexMultiAggregationScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), key);
        } else {
            FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
            return scanOperator.asFullScan().runAsEmbedded(this.txCtxMap.get(Thread.currentThread().threadId()), filterContext);
        }
    }

    @Override
    public MemoryRefNode fetchMemoryReference(Table table, SelectStatement selectStatement) {
        String sqlAsKey = selectStatement.SQL.toString();
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.getOrDefault(sqlAsKey, null);
        List<WherePredicate> wherePredicates;
        if (scanOperator == null) {
            QueryTree queryTree = this.analyzer.analyze(selectStatement);
            wherePredicates = queryTree.wherePredicates;
            scanOperator = this.planner.plan(queryTree);
            this.queryPlanCacheMap.put(sqlAsKey, scanOperator);
        } else {
            wherePredicates = this.analyzer.analyzeWhere(table, selectStatement.whereClause);
        }
        MemoryRefNode memRes;
        if (scanOperator.isIndexScan()) {
            memRes = this.run(wherePredicates, scanOperator.asIndexScan());
        } else if (scanOperator.isIndexAggregationScan()) {
            memRes = this.run(wherePredicates, scanOperator.asIndexAggregationScan());
        } else {
            memRes = this.run(table, wherePredicates, scanOperator.asFullScan());
        }
        return memRes;
    }

    public void issue(Table table, IStatement statement) throws AnalyzerException {
        switch (statement.getType()) {
            case UPDATE -> {
                List<WherePredicate> wherePredicates = this.analyzer.analyzeWhere(table, statement.asUpdateStatement().whereClause);
            }
            case INSERT -> {}
            case DELETE -> {}
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }
    }

    public Object getIndex(String tableName) {
        Table table = this.catalog.get(tableName);
        if (table == null) return null;
        return table.primaryKeyIndex().underlyingIndex();
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QPO-3: projection-aware getScanIterator.
    //
    // When projectedCols is non-null, only those column indices are serialized
    // into the output byte[]. The output size is the sum of the projected
    // columns' byte sizes — NOT the full record size. Sequential offsets are
    // used so VmsResultIterator.parseRowData() reads them correctly.
    //
    // Backward-compatible overload (no projectedCols) delegates to this method
    // with projectedCols=null, preserving existing behaviour for join paths
    // and broadcast paths.
    // ─────────────────────────────────────────────────────────────────────────
    public Iterator<byte[]> getScanIterator(String tableName,
                                            List<SimplePredicate> predicates,
                                            int[] projectedCols,   // QPO-3: null = all columns
                                            long snapshotId) {
        Table table = this.catalog.get(tableName);
        if (table == null) throw new IllegalArgumentException("Table not found: " + tableName);

        var underlying = table.primaryKeyIndex().underlyingIndex();
        if (!(underlying instanceof UniqueHashBufferIndex rawIndex))
            throw new IllegalStateException("getScanIterator requires UniqueHashBufferIndex, got: "
                    + underlying.getClass().getSimpleName());

        // QPO-3: resolve projection parameters
        final int   projectedSize;
        final int[] effectiveCols;

        if (projectedCols != null && projectedCols.length > 0) {
            effectiveCols = projectedCols;
            int size = 0;
            for (int col : projectedCols) size += rawIndex.schema().columnDataType(col).value;
            projectedSize = size;
        } else {
            effectiveCols = null;                                    // full row
            projectedSize = rawIndex.schema().getRecordSizeWithoutHeader();
        }

        LOGGER.log(INFO, ">>> [SCAN] Table: " + tableName
                + " | snapshotId: " + snapshotId
                + " | projectedCols: " + java.util.Arrays.toString(effectiveCols)
                + " | bytes/row: " + projectedSize
                + " | predicates: " + (predicates == null ? "none" : predicates.size()));

        TransactionContext txCtx = new TransactionContext(0, snapshotId, true);
        Iterator<Object[]> iter  = table.primaryKeyIndex().iterator(txCtx);

        return new Iterator<byte[]>() {
            byte[] nextMatch = null;

            @Override
            public boolean hasNext() {
                if (nextMatch != null) return true;
                while (iter.hasNext()) {
                    Object[] row = iter.next();
                    if (row == null) continue;
                    if (predicates == null || predicates.isEmpty() || checkPredicates(row, predicates)) {
                        // QPO-3: serialize only projected columns when effectiveCols != null
                        nextMatch = serializeRowProjected(
                                rawIndex.schema(), row, effectiveCols, projectedSize);
                        return true;
                    }
                }
                return false;
            }

            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext()) throw new NoSuchElementException();
                byte[] result = nextMatch;
                nextMatch = null;
                return result;
            }
        };
    }

    /**
     * Backward-compatible overload — full scan, no projection.
     * All existing callers (join path, broadcast path) use this.
     */
    public Iterator<byte[]> getScanIterator(String tableName,
                                            List<SimplePredicate> predicates,
                                            long snapshotId) {
        return getScanIterator(tableName, predicates, null, snapshotId);
    }

    private boolean checkPredicates(Object[] row, List<SimplePredicate> predicates) {
        for (SimplePredicate p : predicates) {
            Object val = row[p.columnPosition()];
            if (val == null) return false;
            int cmp = compareValues(val, p.value());
            switch (p.expression()) {
                case EQUALS:                if (cmp != 0) return false; break;
                case GREATER_THAN:          if (cmp <= 0) return false; break;
                case LESS_THAN:             if (cmp >= 0) return false; break;
                case GREATER_THAN_OR_EQUAL: if (cmp < 0)  return false; break;
                case LESS_THAN_OR_EQUAL:    if (cmp > 0)  return false; break;
                case NOT_EQUALS:            if (cmp == 0) return false; break;
            }
        }
        return true;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // QPO-3: serialize only the projected columns into a sequential byte[].
    //
    // Output layout: [col[0] bytes][col[1] bytes]...[col[n-1] bytes]
    // Sequential offsets — matches the ColumnDescriptors built in
    // DistributedExecutor with computeDataOffsets(projectedCols).
    // VmsResultIterator.parseRowData() reads using these sequential offsets.
    //
    // When projectedCols is null, delegates to the original full-row
    // serializeRow() — no change in behaviour for existing paths.
    // ─────────────────────────────────────────────────────────────────────────
    private static byte[] serializeRowProjected(
            dk.ku.di.dms.vms.modb.definition.Schema schema,
            Object[] values,
            int[] projectedCols,
            int projectedSize) {

        if (projectedCols == null) {
            // No projection — full row (backward compatible)
            return serializeRow(schema, values,
                    schema.columnOffset(), schema.getRecordSizeWithoutHeader());
        }

        byte[] bytes = new byte[projectedSize];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        int outOffset = 0;

        for (int colIdx : projectedCols) {
            int colSize = schema.columnDataType(colIdx).value;
            if (values[colIdx] == null) {
                outOffset += colSize;
                continue;
            }
            switch (schema.columnDataType(colIdx)) {
                case INT    -> buf.putInt(outOffset, ((Number) values[colIdx]).intValue());
                case LONG   -> buf.putLong(outOffset, ((Number) values[colIdx]).longValue());
                case FLOAT  -> buf.putFloat(outOffset, ((Number) values[colIdx]).floatValue());
                case DOUBLE -> buf.putDouble(outOffset, ((Number) values[colIdx]).doubleValue());
                case DATE   -> {
                    long epoch = (values[colIdx] instanceof java.util.Date d)
                            ? d.getTime() : ((Number) values[colIdx]).longValue();
                    buf.putLong(outOffset, epoch);
                }
                case CHAR, STRING -> {
                    byte[] encoded = values[colIdx].toString().getBytes(StandardCharsets.UTF_8);
                    int maxLen = projectedSize - outOffset;
                    System.arraycopy(encoded, 0, bytes, outOffset,
                            Math.min(encoded.length, maxLen));
                }
                default -> {
                    if (values[colIdx] instanceof Number n)
                        buf.putInt(outOffset, n.intValue());
                }
            }
            outOffset += colSize;
        }
        return bytes;
    }

    public Iterator<byte[]> getJoinIterator(String tableName,
                                            Map<String, byte[]> broadcastBuffer,
                                            int[] localColIndices,
                                            long snapshotId) {
        Table table = this.catalog.get(tableName);
        if (table == null) throw new IllegalArgumentException("Table not found: " + tableName);
        var underlying = table.primaryKeyIndex().underlyingIndex();
        if (!(underlying instanceof UniqueHashBufferIndex rawIndex))
            throw new IllegalStateException("Joins currently require UniqueHashBufferIndex.");
        byte[] localColTypes      = resolveColumnTypes(rawIndex.schema(), localColIndices);
        final int localRecordSize = rawIndex.schema().getRecordSizeWithoutHeader();
        final int[] allColOffsets = rawIndex.schema().columnOffset();
        TransactionContext txCtx = new TransactionContext(0, snapshotId, true);
        Iterator<Object[]> iter  = table.primaryKeyIndex().iterator(txCtx);
        return new Iterator<byte[]>() {
            byte[] nextMatch = null;
            @Override
            public boolean hasNext() {
                if (nextMatch != null) return true;
                try {
                    while (iter.hasNext()) {
                        Object[] localRow = iter.next();
                        if (localRow == null) continue;
                        String key = extractKeyFromRow(localRow, localColIndices, localColTypes);
                        byte[] remoteRow = broadcastBuffer.get(key);
                        if (remoteRow != null) {
                            byte[] localBytes = serializeRow(rawIndex.schema(), localRow, allColOffsets, localRecordSize);
                            nextMatch = new byte[remoteRow.length + localRecordSize];
                            System.arraycopy(remoteRow, 0, nextMatch, 0, remoteRow.length);
                            System.arraycopy(localBytes, 0, nextMatch, remoteRow.length, localRecordSize);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    System.err.println(">>> [JOIN ITERATOR] EXCEPTION in hasNext(): " + e.getClass().getName() + ": " + e.getMessage());
                    e.printStackTrace(System.err);
                    throw e;
                }
                return false;
            }
            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext()) throw new NoSuchElementException();
                byte[] result = nextMatch;
                nextMatch = null;
                return result;
            }
        };
    }

    public Iterator<byte[]> getJoinIteratorFast(String tableName,
                                                Map<Long, byte[]> broadcastBuffer,
                                                int[] localColIndices,
                                                long snapshotId) {
        Table table = this.catalog.get(tableName);
        if (table == null) throw new IllegalArgumentException("Table not found: " + tableName);
        var underlying = table.primaryKeyIndex().underlyingIndex();
        if (!(underlying instanceof UniqueHashBufferIndex rawIndex))
            throw new IllegalStateException("Joins currently require UniqueHashBufferIndex.");
        byte[] localColTypes      = resolveColumnTypes(rawIndex.schema(), localColIndices);
        final int localRecordSize = rawIndex.schema().getRecordSizeWithoutHeader();
        final int[] allColOffsets = rawIndex.schema().columnOffset();
        TransactionContext txCtx = new TransactionContext(0, snapshotId, true);
        Iterator<Object[]> iter  = table.primaryKeyIndex().iterator(txCtx);
        return new Iterator<byte[]>() {
            byte[] nextMatch = null;
            @Override
            public boolean hasNext() {
                if (nextMatch != null) return true;
                try {
                    while (iter.hasNext()) {
                        Object[] localRow = iter.next();
                        if (localRow == null) continue;
                        long key = extractLongKeyFromRow(localRow, localColIndices, localColTypes);
                        byte[] remoteRow = broadcastBuffer.get(key);
                        if (remoteRow != null) {
                            byte[] localBytes = serializeRow(rawIndex.schema(), localRow, allColOffsets, localRecordSize);
                            nextMatch = new byte[remoteRow.length + localRecordSize];
                            System.arraycopy(remoteRow, 0, nextMatch, 0, remoteRow.length);
                            System.arraycopy(localBytes, 0, nextMatch, remoteRow.length, localRecordSize);
                            return true;
                        }
                    }
                } catch (Exception e) {
                    System.err.println(">>> [JOIN ITERATOR FAST] EXCEPTION in hasNext(): " + e.getClass().getName() + ": " + e.getMessage());
                    e.printStackTrace(System.err);
                    throw e;
                }
                return false;
            }
            @Override
            public byte[] next() {
                if (nextMatch == null && !hasNext()) throw new NoSuchElementException();
                byte[] result = nextMatch;
                nextMatch = null;
                return result;
            }
        };
    }

    /**
     * QPO-5: Intra-VMS local hash join.
     *
     * Both tables are co-located in this VMS. Instead of the expensive
     * gateway-side broadcast protocol (2 TCP connections, 100ms delay,
     * full table transfer), we execute the join entirely in local memory.
     *
     * Algorithm:
     *   1. Scan build table (orders), apply buildPredicates, hash on buildJoinCols
     *   2. Scan probe table (order_line), probe hash map on probeJoinCols
     *   3. GROUP BY groupByCol from build table, COUNT(*) matching rows
     *   4. Return result rows: (groupByValue: INT 4 bytes, count: LONG 8 bytes)
     *
     * Cite: DeWitt & Gray 1992 — computation moves to data, not data to computation.
     */
    public Iterator<byte[]> getLocalJoinIterator(
            String buildTableName,
            String probeTableName,
            int[]  buildJoinCols,
            int[]  probeJoinCols,
            List<SimplePredicate> buildPredicates,
            int    groupByCol,
            long   snapshotId) {

        Table buildTable = this.catalog.get(buildTableName);
        Table probeTable = this.catalog.get(probeTableName);
        if (buildTable == null) throw new IllegalArgumentException("Build table not found: " + buildTableName);
        if (probeTable == null) throw new IllegalArgumentException("Probe table not found: " + probeTableName);

        var buildUnderlying = buildTable.primaryKeyIndex().underlyingIndex();
        var probeUnderlying = probeTable.primaryKeyIndex().underlyingIndex();
        if (!(buildUnderlying instanceof UniqueHashBufferIndex buildRaw))
            throw new IllegalStateException("Build table requires UniqueHashBufferIndex");
        if (!(probeUnderlying instanceof UniqueHashBufferIndex probeRaw))
            throw new IllegalStateException("Probe table requires UniqueHashBufferIndex");

        byte[] buildColTypes = resolveColumnTypes(buildRaw.schema(), buildJoinCols);
        byte[] probeColTypes = resolveColumnTypes(probeRaw.schema(), probeJoinCols);

        // ── Step 1: Build phase — hash filtered orders rows ───────────────────
        // Key: composite join key string (o_id-o_d_id-o_w_id)
        // Value: groupByCol value (o_ol_cnt)
        TransactionContext buildCtx = new TransactionContext(0, snapshotId, true);
        Iterator<Object[]> buildIter = buildTable.primaryKeyIndex().iterator(buildCtx);

        Map<String, Integer> buildMap = new HashMap<>();
        while (buildIter.hasNext()) {
            Object[] row = buildIter.next();
            if (row == null) continue;
            if (buildPredicates != null && !buildPredicates.isEmpty()
                    && !checkPredicates(row, buildPredicates)) continue;
            String key = extractKeyFromRow(row, buildJoinCols, buildColTypes);
            buildMap.put(key, ((Number) row[groupByCol]).intValue());
        }

        LOGGER.log(INFO, ">>> [LOCAL JOIN] Build phase complete. " + buildTableName
                + " rows matched: " + buildMap.size() + " | snapshot: " + snapshotId);

        // ── Step 2: Probe phase — scan order_line, match against build map ────
        TransactionContext probeCtx = new TransactionContext(0, snapshotId, true);
        Iterator<Object[]> probeIter = probeTable.primaryKeyIndex().iterator(probeCtx);

        Map<Integer, Long> groups = new HashMap<>();
        while (probeIter.hasNext()) {
            Object[] row = probeIter.next();
            if (row == null) continue;
            String key = extractKeyFromRow(row, probeJoinCols, probeColTypes);
            Integer groupVal = buildMap.get(key);
            if (groupVal != null) {
                groups.merge(groupVal, 1L, Long::sum);
            }
        }

        LOGGER.log(INFO, ">>> [LOCAL JOIN] Probe phase complete. Groups: " + groups.size());

        // ── Step 3: Serialize result rows ─────────────────────────────────────
        // Each result row: (groupByValue: INT 4 bytes, count: LONG 8 bytes) = 12 bytes
        List<byte[]> results = new ArrayList<>(groups.size());
        for (Map.Entry<Integer, Long> entry : groups.entrySet()) {
            ByteBuffer buf = ByteBuffer.allocate(12).order(ByteOrder.nativeOrder());
            buf.putInt(entry.getKey());
            buf.putLong(entry.getValue());
            results.add(buf.array());
        }

        Iterator<byte[]> it = results.iterator();
        return new Iterator<byte[]>() {
            @Override public boolean hasNext() { return it.hasNext(); }
            @Override public byte[] next()     { return it.next();    }
        };
    }

    private static int[] resolveColumnOffsets(dk.ku.di.dms.vms.modb.definition.Schema schema, int[] colIndices) {
        int[] all = schema.columnOffset();
        int[] result = new int[colIndices.length];
        for (int i = 0; i < colIndices.length; i++) result[i] = all[colIndices[i]];
        return result;
    }

    private static byte[] resolveColumnTypes(dk.ku.di.dms.vms.modb.definition.Schema schema, int[] colIndices) {
        byte[] result = new byte[colIndices.length];
        for (int i = 0; i < colIndices.length; i++) {
            result[i] = switch (schema.columnDataType(colIndices[i])) {
                case INT    -> JoinRoutingData.TYPE_INT;
                case LONG   -> JoinRoutingData.TYPE_LONG;
                case DOUBLE -> JoinRoutingData.TYPE_DOUBLE;
                case FLOAT  -> JoinRoutingData.TYPE_FLOAT;
                default     -> JoinRoutingData.TYPE_INT;
            };
        }
        return result;
    }

    private static String extractKeyFromRow(Object[] row, int[] colIndices, byte[] colTypes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colIndices.length; i++) {
            Object val = row[colIndices[i]];
            switch (colTypes[i]) {
                case JoinRoutingData.TYPE_INT    -> sb.append(((Number) val).intValue());
                case JoinRoutingData.TYPE_LONG   -> sb.append(((Number) val).longValue());
                case JoinRoutingData.TYPE_DOUBLE -> sb.append(Double.doubleToRawLongBits(((Number) val).doubleValue()));
                case JoinRoutingData.TYPE_FLOAT  -> sb.append(Float.floatToRawIntBits(((Number) val).floatValue()));
                default                          -> sb.append(((Number) val).intValue());
            }
            if (i < colIndices.length - 1) sb.append('-');
        }
        return sb.toString();
    }

    private static long extractLongKeyFromRow(Object[] row, int[] colIndices, byte[] colTypes) {
        if (colTypes.length == 1) {
            Number val = (Number) row[colIndices[0]];
            return colTypes[0] == JoinRoutingData.TYPE_LONG ? val.longValue() : (long) val.intValue();
        }
        int v0 = ((Number) row[colIndices[0]]).intValue();
        int v1 = ((Number) row[colIndices[1]]).intValue();
        return ((long) v0 << 32) | (v1 & 0xFFFFFFFFL);
    }

    private static byte[] serializeRow(dk.ku.di.dms.vms.modb.definition.Schema schema,
                                       Object[] values, int[] slotRelativeOffsets, int size) {
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) continue;
            int off = slotRelativeOffsets[i] - RECORD_HEADER;
            if (off < 0 || off >= size) continue;
            switch (schema.columnDataType(i)) {
                case INT    -> buf.putInt(off, ((Number) values[i]).intValue());
                case LONG   -> buf.putLong(off, ((Number) values[i]).longValue());
                case FLOAT  -> buf.putFloat(off, ((Number) values[i]).floatValue());
                case DOUBLE -> buf.putDouble(off, ((Number) values[i]).doubleValue());
                case DATE   -> {
                    long epoch = (values[i] instanceof java.util.Date d) ? d.getTime() : ((Number) values[i]).longValue();
                    buf.putLong(off, epoch);
                }
                case CHAR, STRING -> {
                    byte[] encoded = values[i].toString().getBytes(StandardCharsets.UTF_8);
                    int maxLen = size - off;
                    System.arraycopy(encoded, 0, bytes, off, Math.min(encoded.length, maxLen));
                }
                default -> { if (values[i] instanceof Number n) buf.putInt(off, n.intValue()); }
            }
        }
        return bytes;
    }

    private int compareValues(Object val1, Object val2) {
        if (val1 instanceof Number n1 && val2 instanceof Number n2) {
            return Double.compare(n1.doubleValue(), n2.doubleValue());
        }
        return String.valueOf(val1).compareTo(String.valueOf(val2));
    }

    @Override
    public List<Object[]> getAll(Table table) {
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = table.primaryKeyIndex().iterator(this.txCtxMap.get(Thread.currentThread().threadId()));
        while (iterator.hasNext()) res.add(iterator.next());
        return res;
    }

    @Override
    public void insertAll(Table table, List<Object[]> objects) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for (Object[] entry : objects) this.doInsert(txCtx, table, entry);
    }

    @Override
    public void deleteAll(Table table, List<Object[]> objects) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for (Object[] entry : objects) {
            IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), entry);
            this.deleteByKey(txCtx, table, pk);
        }
    }

    @Override
    public void updateAll(Table table, List<Object[]> objects) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        for (Object[] entry : objects) this.update(txCtx, table, entry);
    }

    @Override
    public void delete(Table table, Object[] values) {
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), values);
        this.deleteByKey(txCtx, table, pk);
    }

    @Override
    public void deleteByKey(Table table, Object[] keyValues) {
        IKey pk = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), keyValues);
        this.deleteByKey(this.txCtxMap.get(Thread.currentThread().threadId()), table, pk);
    }

    private void deleteByKey(TransactionContext txCtx, Table table, IKey pk) {
        Optional<Object[]> opt = table.primaryKeyIndex().removeOpt(txCtx, pk);
        if (opt.isPresent()) {
            txCtx.indexes.add(table.primaryKeyIndex());
            for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                txCtx.indexes.add(secIndex);
                secIndex.remove(txCtx, pk, opt.get());
            }
            for (var entry : table.partialIndexMap.entrySet()) {
                Tuple<Integer, Object> check = table.partialIndexMetaMap.get(entry.getKey());
                if (table.primaryKeyIndex().meetPartialIndex(opt.get(), check.t1(), check.t2())) {
                    txCtx.indexes.add(entry.getValue());
                    entry.getValue().remove(txCtx, pk);
                }
            }
        }
    }

    @Override
    public boolean exists(PrimaryIndex index, Object[] valuesOfKey) {
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), valuesOfKey);
        return index.exists(this.txCtxMap.get(Thread.currentThread().threadId()), pk);
    }

    @Override
    public Object[] lookupByKey(PrimaryIndex index, Object[] valuesOfKey) {
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), valuesOfKey);
        return index.lookupByKey(this.txCtxMap.get(Thread.currentThread().threadId()), pk);
    }

    @Override
    public void insert(Table table, Object[] values) {
        this.doInsert(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    private Object[] doInsert(TransactionContext txCtx, Table table, Object[] values) {
        PrimaryIndex primaryIndex = table.primaryKeyIndex();
        if (this.fkConstraintViolation(txCtx, table, values)) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Foreign key constraint violation in table " + table.getName());
        }
        IKey pk = primaryIndex.insertAndGetKey(txCtx, values);
        if (pk == null) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Constraint violation in table " + table.getName() + ". Record:\n" + Arrays.stream(values).toList());
        }
        trackIndexes(txCtx, table, values, primaryIndex, pk);
        return values;
    }

    private static void trackIndexes(TransactionContext txCtx, Table table, Object[] values, PrimaryIndex primaryIndex, IKey pk) {
        txCtx.indexes.add(primaryIndex);
        for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
            txCtx.indexes.add(secIndex);
            secIndex.insert(txCtx, pk, values);
        }
        if (table.partialIndexMap.isEmpty()) return;
        for (var entry : table.partialIndexMap.entrySet()) {
            Tuple<Integer, Object> check = table.partialIndexMetaMap.get(entry.getKey());
            if (primaryIndex.meetPartialIndex(values, check.t1(), check.t2())) {
                txCtx.indexes.add(entry.getValue());
                entry.getValue().insert(txCtx, pk, values);
            }
        }
    }

    @Override
    public Object[] insertAndGet(Table table, Object[] values) {
        return this.doInsert(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    @Override
    public void upsert(Table table, Object[] values) {
        PrimaryIndex primaryIndex = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(primaryIndex.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        TransactionContext txCtx = this.txCtxMap.get(Thread.currentThread().threadId());
        if (primaryIndex.upsert(txCtx, pk, values)) {
            trackIndexes(txCtx, table, values, primaryIndex, pk);
            return;
        }
        this.undoTransactionWrites(txCtx);
        throw new RuntimeException("Constraint violation.");
    }

    @Override
    public void update(Table table, Object[] values) {
        this.update(this.txCtxMap.get(Thread.currentThread().threadId()), table, values);
    }

    private void update(TransactionContext txCtx, Table table, Object[] values) {
        PrimaryIndex index = table.primaryKeyIndex();
        IKey pk = KeyUtils.buildRecordKey(index.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        if (!index.update(txCtx, pk, values)) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Primary key constraint violation. Table: " + table.getName() + " Key: " + pk);
        }
        if (this.fkConstraintViolation(txCtx, table, values)) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Foreign key constraint violation. Table: " + table.getName() + " Key: " + pk);
        }
        txCtx.indexes.add(index);
    }

    private void undoTransactionWrites(TransactionContext txCtx) {
        for (IMultiVersionIndex index : txCtx.indexes) index.undoTransactionWrites(txCtx);
    }

    public MemoryRefNode run(List<WherePredicate> wherePredicates, IndexAggregateScan operator) { return null; }
    public MemoryRefNode run(List<WherePredicate> wherePredicates, IndexScan operator) { return null; }
    public MemoryRefNode run(Table table, List<WherePredicate> wherePredicates, FullScan operator) {
        FilterContext filterContext = FilterContextBuilder.build(wherePredicates);
        return null;
    }

    private IKey[] getMultiKeysFromWhereClause(WherePredicate wherePredicate) {
        if (wherePredicate.value instanceof int[] intArray) {
            SimpleKey[] keysToRet = new SimpleKey[intArray.length];
            int idx = 0;
            for (var key : intArray) { keysToRet[idx] = SimpleKey.of(key); idx++; }
            return keysToRet;
        }
        throw new RuntimeException("Do not support IN clause of types other than INT");
    }

    private IKey getIndexedKeysFromWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index) {
        int i = 0;
        Object[] keyList = new Object[index.indexColumns().length];
        for (WherePredicate wherePredicate : wherePredicates) {
            if (index.containsColumn(wherePredicate.columnReference.columnPosition)) {
                keyList[i] = wherePredicate.value;
                i++;
            }
        }
        return KeyUtils.buildRecordKey(keyList);
    }

    private List<WherePredicate> getNonIndexedColumnsWhereClause(List<WherePredicate> wherePredicates, IMultiVersionIndex index) {
        List<WherePredicate> nonIdxWhereClause = new ArrayList<>();
        for (WherePredicate wherePredicate : wherePredicates) {
            if (index.containsColumn(wherePredicate.columnReference.columnPosition)) continue;
            nonIdxWhereClause.add(wherePredicate);
        }
        return nonIdxWhereClause;
    }

    @Override
    public void checkpoint(long maxTid) {
        LOGGER.log(DEBUG, "Checkpoint for max TID " + maxTid + " started at " + System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            int numRecords = table.primaryKeyIndex().checkpoint(maxTid);
            if (numRecords > 0) LOGGER.log(DEBUG, numRecords + " record(s) persisted to table " + table.getName());
            else LOGGER.log(DEBUG, "No records have been flushed to table " + table.getName());
        }
        LOGGER.log(DEBUG, "Checkpoint for max TID " + maxTid + " finished at " + System.currentTimeMillis());
    }

    @Override
    public void cleanup(long maxTid) {
        LOGGER.log(DEBUG, "Garbage collection for max TID " + maxTid + " started at " + System.currentTimeMillis());
        for (Table table : this.catalog.values()) table.primaryKeyIndex().cleanup(maxTid);
        LOGGER.log(DEBUG, "Garbage collection for max TID " + maxTid + " finished at " + System.currentTimeMillis());
    }

    @Override
    public void commit() {
        TransactionContext txCtx = this.txCtxMap.remove(Thread.currentThread().threadId());
        for (IMultiVersionIndex index : txCtx.indexes) index.installWrites(txCtx);
    }

    @Override
    public ITransactionContext beginTransaction(long tid, int identifier, long lastTid, boolean readOnly) {
        return this.txCtxMap.compute(Thread.currentThread().threadId(), (_, v) -> {
            if (v != null && v.tid == 0 && tid == 0) return v;
            return new TransactionContext(tid, lastTid, readOnly);
        });
    }

    @Override
    public void reset() {
        LOGGER.log(DEBUG, "Reset triggered at " + System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            LOGGER.log(DEBUG, "Resetting " + table.name);
            table.primaryKeyIndex().reset();
            for (NonUniqueSecondaryIndex secIdx : table.secondaryIndexMap.values()) secIdx.reset();
            for (UniqueSecondaryIndex uniqueIdx : table.partialIndexMap.values()) uniqueIdx.reset();
        }
        LOGGER.log(DEBUG, "Reset finished at " + System.currentTimeMillis());
    }

    @Override
    public void rebuildIndexes() {
        TransactionContext txCtx = (TransactionContext) beginTransaction(0, 0, 0, false);
        for (Table table : this.catalog.values()) {
            int count = 0;
            Iterator<Object[]> it = table.primaryKeyIndex().iterator(txCtx);
            while (it.hasNext()) {
                Object[] record = it.next();
                IKey key = KeyUtils.buildRecordKey(table.schema().getPrimaryKeyColumns(), record);
                table.primaryKeyIndex().doInsert(txCtx, key, record, null);
                for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values()) {
                    secIndex.insert(txCtx, key, record);
                }
                count++;
            }
            LOGGER.log(INFO, "Table " + table.getName() + " with " + count + " entries scanned for index rebuilding.");
        }
    }
}