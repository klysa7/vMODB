package dk.ku.di.dms.vms.modb.transaction;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.statement.IStatement;
import dk.ku.di.dms.vms.modb.api.query.statement.SelectStatement;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.memory.MemoryRefNode;
import dk.ku.di.dms.vms.modb.common.schema.network.query.JoinRoutingData;
import dk.ku.di.dms.vms.modb.definition.Header;
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

/**
 * A transaction management facade.
 * For OLAP joins, getJoinIterator() uses the MVCC-aware primaryIndex.iterator(txCtx)
 * so that all committed records are visible, including bulk-populated records not yet
 * checkpointed to the raw UniqueHashBufferIndex buffer.
 */
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
        AbstractSimpleOperator scanOperator = this.queryPlanCacheMap.computeIfAbsent(sqlAsKey,
                (_) -> {
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
            case UPDATE -> { this.analyzer.analyzeWhere(table, statement.asUpdateStatement().whereClause); }
            case INSERT -> { }
            case DELETE -> { }
            default -> throw new IllegalStateException("Statement type cannot be identified.");
        }
    }

    public Object getIndex(String tableName) {
        Table table = this.catalog.get(tableName);
        if (table == null) return null;
        return table.primaryKeyIndex().underlyingIndex();
    }

    public Iterator<Long> getScanIterator(String tableName, List<SimplePredicate> predicates) {
        Table table = this.catalog.get(tableName);
        if (table == null) throw new IllegalArgumentException("Table not found: " + tableName);

        LOGGER.log(INFO, ">>> [SCAN] Table: " + tableName + " | predicates: "
                + (predicates == null ? "none" : predicates.size() + " → " + predicates));

        var underlying = table.primaryKeyIndex().underlyingIndex();

        if (underlying instanceof UniqueHashBufferIndex rawIndex) {
            IRecordIterator<IKey> internalIterator = rawIndex.iterator();
            return new Iterator<Long>() {
                Long nextMatch = null;
                @Override
                public boolean hasNext() {
                    if (nextMatch != null) return true;
                    while (internalIterator.hasNext()) {
                        internalIterator.next();
                        long currentAddr = internalIterator.address();
                        if (predicates == null || predicates.isEmpty()) {
                            nextMatch = currentAddr;
                            return true;
                        }
                        Object[] record = rawIndex.record(internalIterator);
                        if (record == null) continue;
                        if (checkPredicates(record, predicates)) {
                            nextMatch = currentAddr;
                            return true;
                        }
                    }
                    return false;
                }
                @Override
                public Long next() {
                    if (nextMatch == null && !hasNext()) throw new NoSuchElementException();
                    Long result = nextMatch;
                    nextMatch = null;
                    return result;
                }
            };
        }

        LOGGER.log(INFO, "Table " + tableName + " is using HashMapIndex. Attempting fallback...");
        try {
            java.lang.reflect.Method method = underlying.getClass().getMethod("addressIterator");
            return (Iterator<Long>) method.invoke(underlying);
        } catch (Exception e) {
            throw new IllegalStateException("Index does not support address iteration.");
        }
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

    /**
     * Returns an iterator over joined byte arrays [remotePayload | localPayload].
     *
     * Active-record detection delegates to rawIndex.record() — the same mechanism
     * used by getScanIterator — rather than a raw ACTIVE_BYTE byte check.
     * This guarantees consistent slot-skip semantics regardless of how
     * UniqueHashBufferIndex lays out its header bytes.
     *
     * @param tableName       local (probe-side) table name
     * @param broadcastBuffer build-side map: composite String key → remote row bytes
     * @param localColIndices column indices in the local Schema that form the join key
     */
    /**
     * Returns an iterator over joined byte arrays [remotePayload | localPayload].
     *
     * Uses the MVCC-aware {@code primaryIndex.iterator(txCtx)} so that all committed
     * records are visible — including bulk-populated records that have not yet been
     * checkpointed to the raw {@code UniqueHashBufferIndex} buffer.
     *
     * Using {@code rawIndex.iterator()} directly only sees checkpointed records, which
     * caused silently incomplete results (~270 rows instead of ~30,000 for orders).
     *
     * The read-only {@code TransactionContext(tid=0, lastTid=0, readOnly=true)} mirrors
     * the pattern used in {@code rebuildIndexes()} and grants visibility to all committed
     * writes without registering the context in {@code txCtxMap}.
     *
     * @param tableName       local (probe-side) table name
     * @param broadcastBuffer build-side map: composite String key → remote row bytes
     * @param localColIndices column indices in the local Schema that form the join key
     */
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
        final int[] allColOffsets = rawIndex.schema().columnOffset(); // slot-relative, used for serialization

        // Read-only TransactionContext: sees all committed records including non-checkpointed
        // bulk inserts.  Not registered in txCtxMap — OLAP workers are transient threads.
        // snapshotId from Coordinator: only show records committed up to this TID
        // so both VMSes in the join read from the same consistent global snapshot.
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
                            byte[] localBytes = serializeRow(rawIndex.schema(), localRow,
                                    allColOffsets, localRecordSize);

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

    /**
     * Fast-path overload: uses a {@code Map<Long, byte[]>} broadcast buffer built
     * with {@code extractLongKey} — no String allocation per row.
     * Only valid when {@code canUseLongKey} returned {@code true}
     * for the local column types (1 INT/LONG col, or 2 INT cols).
     *
     * @param tableName       local (probe-side) table name
     * @param broadcastBuffer build-side map: packed long key → remote row bytes
     * @param localColIndices column indices in the local Schema that form the join key
     */
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

        // Same consistent snapshot as getJoinIterator — bounded to snapshotId.
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
                            byte[] localBytes = serializeRow(rawIndex.schema(), localRow,
                                    allColOffsets, localRecordSize);

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
     * Returns the byte offset of each requested column from the start of the row
     * payload (after RECORD_HEADER). Uses Schema.columnOffset() — a pre-computed
     * array maintained by Schema that is data-relative (does not include the header).
     */
    private static int[] resolveColumnOffsets(dk.ku.di.dms.vms.modb.definition.Schema schema,
                                              int[] colIndices) {
        int[] all = schema.columnOffset();
        int[] result = new int[colIndices.length];
        for (int i = 0; i < colIndices.length; i++) result[i] = all[colIndices[i]];
        return result;
    }

    /**
     * Maps each requested column's DataType to a JoinRoutingData type code.
     */
    private static byte[] resolveColumnTypes(dk.ku.di.dms.vms.modb.definition.Schema schema,
                                             int[] colIndices) {
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

    /**
     * Extracts a composite '-'-delimited String join key from a materialised Object[] row.
     * Symmetric with {@code extractKey} on the build side,
     * on the build side, but reads from an Object[] rather than raw memory.
     */
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

    /**
     * Extracts a packed {@code long} join key from an Object[] row.
     * Only valid when {@code canUseLongKey} returned {@code true} for the same {@code colTypes}.
     */
    private static long extractLongKeyFromRow(Object[] row, int[] colIndices, byte[] colTypes) {
        if (colTypes.length == 1) {
            Number val = (Number) row[colIndices[0]];
            return colTypes[0] == JoinRoutingData.TYPE_LONG
                    ? val.longValue()
                    : (long) val.intValue();
        }
        // 2 INT columns — pack into one long, same packing as build-side extractLongKey
        int v0 = ((Number) row[colIndices[0]]).intValue();
        int v1 = ((Number) row[colIndices[1]]).intValue();
        return ((long) v0 << 32) | (v1 & 0xFFFFFFFFL);
    }

    /**
     * Serialises an Object[] record to a byte[] in the VMS native-endian wire format,
     * producing the same layout that {@code UNSAFE.copyMemory(addr + RECORD_HEADER, ...)}
     * previously copied directly from the off-heap slot.
     *
     * {@code slotRelativeOffsets} are the values from {@code schema.columnOffset()} — they
     * include {@code RECORD_HEADER} (= 5 bytes). Subtracting {@code RECORD_HEADER} converts
     * them to data-relative positions within the returned byte array.
     *
     * {@link ByteOrder#nativeOrder()} is used so the endianness matches what
     * {@code ByteBuffer.wrap(rowData).order(ByteOrder.nativeOrder())} reads in
     * {@code VmsResultIterator.parseRowData()}.
     */
    private static byte[] serializeRow(dk.ku.di.dms.vms.modb.definition.Schema schema,
                                       Object[] values,
                                       int[] slotRelativeOffsets,
                                       int size) {
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) continue;
            int off = slotRelativeOffsets[i] - RECORD_HEADER; // data-relative
            if (off < 0 || off >= size) continue;            // safety guard
            switch (schema.columnDataType(i)) {
                case INT                        -> buf.putInt(off, ((Number) values[i]).intValue());
                case LONG                       -> buf.putLong(off, ((Number) values[i]).longValue());
                case FLOAT                      -> buf.putFloat(off, ((Number) values[i]).floatValue());
                case DOUBLE                     -> buf.putDouble(off, ((Number) values[i]).doubleValue());
                case DATE -> {
                    // VMS stores DATE/TIMESTAMP as epoch millis (long) in the off-heap slot.
                    // The MVCC iterator returns them as java.util.Date — extract getTime().
                    long epoch = (values[i] instanceof java.util.Date d)
                            ? d.getTime()
                            : ((Number) values[i]).longValue();
                    buf.putLong(off, epoch);
                }
                case CHAR -> {
                    // VMS stores CHAR as UTF-16LE in the off-heap slot.
                    byte[] encoded = values[i].toString().getBytes(StandardCharsets.UTF_16LE);
                    int maxLen = size - off;
                    System.arraycopy(encoded, 0, bytes, off, Math.min(encoded.length, maxLen));
                }
                default -> {
                    // Fallback for any numeric type not explicitly listed above.
                    if (values[i] instanceof Number n) buf.putInt(off, n.intValue());
                }
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

    /****** ENTITY *******/

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
            throw new RuntimeException("Constraint violation in table " + table.getName()
                    + ". Record:\n" + Arrays.stream(values).toList());
        }
        trackIndexes(txCtx, table, values, primaryIndex, pk);
        return values;
    }

    private static void trackIndexes(TransactionContext txCtx, Table table, Object[] values,
                                     PrimaryIndex primaryIndex, IKey pk) {
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
        IKey pk = KeyUtils.buildRecordKey(
                primaryIndex.underlyingIndex().schema().getPrimaryKeyColumns(), values);
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
        IKey pk = KeyUtils.buildRecordKey(
                index.underlyingIndex().schema().getPrimaryKeyColumns(), values);
        if (!index.update(txCtx, pk, values)) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Primary key constraint violation. Table: "
                    + table.getName() + " Key: " + pk);
        }
        if (this.fkConstraintViolation(txCtx, table, values)) {
            this.undoTransactionWrites(txCtx);
            throw new RuntimeException("Foreign key constraint violation. Table: "
                    + table.getName() + " Key: " + pk);
        }
        txCtx.indexes.add(index);
    }

    private void undoTransactionWrites(TransactionContext txCtx) {
        for (IMultiVersionIndex index : txCtx.indexes) index.undoTransactionWrites(txCtx);
    }

    /****** SCAN OPERATORS *******/

    public MemoryRefNode run(List<WherePredicate> wherePredicates, IndexAggregateScan operator) {
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

    private IKey getIndexedKeysFromWhereClause(List<WherePredicate> wherePredicates,
                                               IMultiVersionIndex index) {
        int i = 0;
        Object[] keyList = new Object[index.indexColumns().length];
        for (WherePredicate wp : wherePredicates) {
            if (index.containsColumn(wp.columnReference.columnPosition)) {
                keyList[i] = wp.value;
                i++;
            }
        }
        return KeyUtils.buildRecordKey(keyList);
    }

    private List<WherePredicate> getNonIndexedColumnsWhereClause(List<WherePredicate> wherePredicates,
                                                                 IMultiVersionIndex index) {
        List<WherePredicate> nonIdxWhereClause = new ArrayList<>();
        for (WherePredicate wp : wherePredicates) {
            if (!index.containsColumn(wp.columnReference.columnPosition))
                nonIdxWhereClause.add(wp);
        }
        return nonIdxWhereClause;
    }

    public MemoryRefNode run(List<WherePredicate> wherePredicates, IndexScan operator) { return null; }

    public MemoryRefNode run(Table table, List<WherePredicate> wherePredicates, FullScan operator) {
        FilterContextBuilder.build(wherePredicates);
        return null;
    }

    @Override
    public void checkpoint(long maxTid) {
        LOGGER.log(DEBUG, "Checkpoint for max TID " + maxTid + " started at " + System.currentTimeMillis());
        for (Table table : this.catalog.values()) {
            int numRecords = table.primaryKeyIndex().checkpoint(maxTid);
            if (numRecords > 0) LOGGER.log(DEBUG, numRecords + " record(s) persisted to table " + table.getName());
            else LOGGER.log(DEBUG, "No records flushed to table " + table.getName());
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
        return this.txCtxMap.compute(Thread.currentThread().threadId(),
                (_, v) -> {
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
                for (NonUniqueSecondaryIndex secIndex : table.secondaryIndexMap.values())
                    secIndex.insert(txCtx, key, record);
                count++;
            }
            LOGGER.log(INFO, "Table " + table.getName() + " with " + count + " entries scanned for index rebuilding.");
        }
    }
}