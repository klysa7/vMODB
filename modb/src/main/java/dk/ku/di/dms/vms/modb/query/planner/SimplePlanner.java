package dk.ku.di.dms.vms.modb.query.planner;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.api.query.enums.GroupByOperationEnum;
import dk.ku.di.dms.vms.modb.definition.ColumnReference;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.definition.key.KeyUtils;
import dk.ku.di.dms.vms.modb.index.IIndexKey;
import dk.ku.di.dms.vms.modb.index.interfaces.ReadWriteIndex;
import dk.ku.di.dms.vms.modb.query.analyzer.QueryTree;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.GroupByPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.JoinPredicate;
import dk.ku.di.dms.vms.modb.query.analyzer.predicate.WherePredicate;
import dk.ku.di.dms.vms.modb.query.execution.operators.AbstractSimpleOperator;
import dk.ku.di.dms.vms.modb.query.execution.operators.IndexMultiAggregateScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCount;
import dk.ku.di.dms.vms.modb.query.execution.operators.count.IndexCountGroupBy;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexGroupByMax;
import dk.ku.di.dms.vms.modb.query.execution.operators.minmax.IndexGroupByMin;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.*;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.IndexSum;
import dk.ku.di.dms.vms.modb.query.execution.operators.sum.Sum;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static java.lang.System.Logger.Level.INFO;

/**
 * Planner that only takes into consideration simple read and write queries.
 * Those involving single tables and filters, and no
 * multiple aggregations or joins with more than 2 tables.
 */
public final class SimplePlanner {

    public SimplePlanner() {
    }

    private static final System.Logger LOGGER = System.getLogger(SimplePlanner.class.getName());

    /**
     * @param columnsToFilter additional columns to be filtered, but not on index
     */
    private record IndexSelectionVerdict(boolean indexIsUsedGivenWhereClause,
                                         IMultiVersionIndex index,
                                         int[] columnsToFilter) implements Comparable<IndexSelectionVerdict> {

        @Override
        public int compareTo(IndexSelectionVerdict o) {
            // do both indexes can be effectively applied?
//            if (this.indexIsUsedGivenWhereClause && o.indexIsUsedGivenWhereClause) {
//
//                // are both unique?
//                if (this.index.getType() == IndexTypeEnum.UNIQUE && o.index.getType() == IndexTypeEnum.UNIQUE) {
//                    // which one has more columns to filter? leading to more rows cut
//                    // that may not be correct given the selectivity of some values, simple heuristic here
//                    if (this.columnsToFilter.length > o.columnsToFilter.length) return 0;
//                    if (this.columnsToFilter.length < o.columnsToFilter.length) return 1;
//
//                    // which one has larger table?
//                    if (this.index.size() < o.index.size()) return 0;
//
//                } else if (this.index.getType() == IndexTypeEnum.UNIQUE) return 0;
//
//            } else if (this.indexIsUsedGivenWhereClause) {
//                return 0; // must be deep left then
//            }
            return 1;
        }

    }

    public AbstractSimpleOperator plan(QueryTree queryTree) {

        LOGGER.log(INFO,
                """
                        ===== ENTERING QUERY PLANNER =====
                          QueryTree            = %s
                          isSimpleScan          = %s
                          isSimpleScanWithOrder = %s
                          isSimpleAggregate     = %s
                          isSimpleJoin          = %s
                          hasMultipleJoins      = %s
                          hasMultipleAggregates = %s
                        """.formatted(
                        queryTree,
                        queryTree.isSimpleScan(),
                        queryTree.isSimpleScanWithOrder(),
                        queryTree.isSimpleAggregate(),
                        queryTree.isSimpleJoin(),
                        queryTree.hasMultipleJoins(),
                        queryTree.hasMultipleAggregates()
                )
        );

        if (queryTree.isSimpleScan()) {
            LOGGER.log(INFO, "PLAN DECISION → planSimpleScan");
            return this.planSimpleScan(queryTree);
        }

        if (queryTree.isSimpleScanWithOrder()) {
            LOGGER.log(INFO, "PLAN DECISION → planSimpleScanWithOrder");
            return this.planSimpleScanWithOrder(queryTree);
        }

        if (queryTree.isSimpleAggregate()) {
            LOGGER.log(INFO, "PLAN DECISION → planSimpleAggregate");
            return this.planSimpleAggregate(queryTree);
        }

        if (queryTree.isSimpleJoin()) {
            LOGGER.log(INFO, "PLAN DECISION → planSimpleJoin");
            return this.planSimpleJoin(queryTree);
        }

        if (queryTree.hasMultipleJoins()) {
            LOGGER.log(INFO, "PLAN DECISION → planMultipleJoins");
            return this.planMultipleJoins(queryTree);
        }

        if (queryTree.hasMultipleAggregates()) {
            LOGGER.log(INFO, "PLAN DECISION → planMultipleAggregates");
            return this.planMultipleAggregates(queryTree);
        }

        LOGGER.log(INFO,
                """
                        ❌ NO PLAN MATCHED
                          QueryTree = %s
                        """.formatted(queryTree)
        );

        throw new RuntimeException("Could not find a plan for :" + queryTree);
    }

    private AbstractSimpleOperator planMultipleAggregates(QueryTree queryTree) {
        IMultiVersionIndex indexSelected = this.getOptimalIndex(
                queryTree.projections.getFirst().table,
                queryTree.wherePredicates).index();
        return new IndexMultiAggregateScan(queryTree.groupByProjections, indexSelected, queryTree.projections.stream().map(i -> i.columnPosition).toList(), 0);
    }

    private AbstractSimpleOperator planSimpleJoin(QueryTree queryTree) {

        // define left deep
        JoinPredicate joinPredicate = queryTree.joinPredicates.getFirst();

        // the index with most columns applying to the probe is the deepest
        IndexSelectionVerdict indexForTable1 = this.getOptimalIndex(joinPredicate.getLeftTable(), queryTree.wherePredicates);

        IndexSelectionVerdict indexForTable2 = this.getOptimalIndex(joinPredicate.getRightTable(), queryTree.wherePredicates);

        // TODO finish

//        if(indexForTable1.compareTo( indexForTable2 ) == 0){
//            // should be left
//            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE && indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinWithProjection( indexForTable1.index, indexForTable2.index,
//                    null, null,
//                    null, null,
//                    null, 0 );
//            if(indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable1.index, indexForTable2.index,
//                        null, null,
//                        null, null,
//                        null, 0 );
//            throw new IllegalStateException("No support for join on non unique hash indexes!");
//        } else {
//            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE && indexForTable1.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinWithProjection( indexForTable2.index, indexForTable1.index,
//                    null, null,
//                    null, null,
//                    null, 0 );
//            if(indexForTable2.index.getType() == IndexTypeEnum.UNIQUE)
//                return new UniqueHashJoinNonUniqueHashWithProjection( indexForTable2.index, indexForTable1.index,
//                        null, null,
//                        null, null,
//                        null, 0 );
//            throw new IllegalStateException("No support for join on non unique hash indexes!");
//        }
        return null;
    }

    private AbstractSimpleOperator planMultipleJoins(QueryTree queryTree) {
        // order joins in order of join operation
        // simple heuristic, table with most records goes first, but care must be taken on precedence of foreign keys
        // dynamic programming. which join must execute first?
        // ordered by the number of records? index type?
        return null;
    }

    private AbstractSimpleOperator planSimpleAggregate(QueryTree queryTree) {
        // then just one since it is simple
        var op = queryTree.groupByProjections.getFirst().groupByOperation();
        switch (op) {
            case MIN, MAX -> {
                Table tb = queryTree.groupByProjections.getFirst().columnReference().table;
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates).index();
                int[] indexColumns = queryTree.groupByColumns.stream()
                        .mapToInt(ColumnReference::getColumnPosition).toArray();

                // assumed to be only one
                int minMaxColumn = queryTree.groupByProjections.stream().mapToInt(GroupByPredicate::columnPosition).toArray()[0];

                final int[] projectionColumns = new int[queryTree.projections.size() + 1];
                int idxCol = 0;
                for (var column : queryTree.projections) {
                    projectionColumns[idxCol] = column.getColumnPosition();
                    idxCol++;
                }
                projectionColumns[idxCol] = minMaxColumn;

                // calculate entry size... sum of the size of the column types of the projection
                int entrySize = calculateQueryResultEntrySize(tb.schema(), queryTree.projections.size() + 1, projectionColumns);

                if (op == GroupByOperationEnum.MIN) {
                    return new IndexGroupByMin(entrySize, indexSelected, projectionColumns, tb.schema(),
                            indexColumns, minMaxColumn, queryTree.limit.orElse(Integer.MAX_VALUE));
                } else {
                    return new IndexGroupByMax(entrySize, indexSelected, projectionColumns, tb.schema(),
                            indexColumns, minMaxColumn, queryTree.limit.orElse(Integer.MAX_VALUE));
                }
            }
            case SUM -> {
                // check if there is an index that can be applied
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates).index();
                if (indexSelected == null) {
                    return new Sum(
                            queryTree.groupByProjections.getFirst().columnReference().dataType,
                            queryTree.groupByProjections.getFirst().columnReference().columnPosition,
                            queryTree.groupByProjections.getFirst().columnReference().table.underlyingPrimaryKeyIndex());
                }
                return new IndexSum(
                        queryTree.groupByProjections.getFirst().columnReference().dataType,
                        queryTree.groupByProjections.getFirst().columnReference().columnPosition,
                        // indexSelected
                        queryTree.groupByProjections.getFirst().columnReference().table.underlyingPrimaryKeyIndex()
                );
            }
            case COUNT -> {
                Table tb = queryTree.groupByProjections.getFirst().columnReference().table;
                IMultiVersionIndex indexSelected = this.getOptimalIndex(
                        queryTree.groupByProjections.getFirst().columnReference().table,
                        queryTree.wherePredicates
                ).index();
                if (queryTree.groupByColumns.isEmpty()) {
                    // then no group by
                    // how the user can specify a distinct?
                    return new IndexCount(
//                            indexSelected == null ? tb.underlyingPrimaryKeyIndex() :  indexSelected
                            tb.underlyingPrimaryKeyIndex()
                    );
                } else {
                    int[] columns = queryTree.groupByColumns.stream()
                            .mapToInt(ColumnReference::getColumnPosition).toArray();
                    return new IndexCountGroupBy(
                            // indexSelected == null ? tb.underlyingPrimaryKeyIndex() : indexSelected,
                            tb.underlyingPrimaryKeyIndex(),
                            columns
                    );
                }
            }
            default -> throw new IllegalStateException("Operator not yet implemented.");
        }
    }

    private AbstractScan planSimpleScanWithOrder(QueryTree queryTree) {
        Table tb = queryTree.projections.getFirst().table;
        IndexSelectionVerdict indexSelectionVerdict = this.getOptimalIndex(tb, queryTree.wherePredicates);
        int[] projectionColumns = new int[queryTree.projections.size()];
        int idxCol = 0;
        for (var column : queryTree.projections) {
            projectionColumns[idxCol] = column.getColumnPosition();
            idxCol++;
        }
        int entrySize = calculateQueryResultEntrySize(tb.schema(), queryTree.projections.size(), projectionColumns);
        if (indexSelectionVerdict.indexIsUsedGivenWhereClause()) {
            return new IndexScanWithOrder(indexSelectionVerdict.index(), projectionColumns,
                    queryTree.orderByPredicates.getFirst().columnReference.columnPosition, entrySize);
        } else {
            return new FullScanWithOrder(tb.primaryKeyIndex(), projectionColumns,
                    queryTree.orderByPredicates.getFirst().columnReference.columnPosition, entrySize);
        }
    }

    private AbstractScan planSimpleScan(QueryTree queryTree) {

        LOGGER.log(INFO,
                """
                        ---- ENTER planSimpleScan ----
                          projections.size     = %d
                          wherePredicates.size = %d
                          projections          = %s
                          wherePredicates      = %s
                        """.formatted(
                        queryTree.projections.size(),
                        queryTree.wherePredicates == null ? -1 : queryTree.wherePredicates.size(),
                        queryTree.projections,
                        queryTree.wherePredicates
                )
        );

        // given it is simple, pick the table from one of the columns
        // must always have at least one projected column
        Table tb = queryTree.projections.getFirst().table;

        LOGGER.log(INFO,
                """
                        Selected table:
                          table.name     = %s
                          table.schema   = %s
                          primaryKeyIdx  = %s
                        """.formatted(
                        tb.name,
                        tb.schema(),
                        tb.primaryKeyIndex()
                )
        );

        // avoid one of the columns to have expression different from EQUALS
        // to be picked by unique and non-unique index
        IndexSelectionVerdict indexSelectionVerdict =
                this.getOptimalIndex(tb, queryTree.wherePredicates);

        LOGGER.log(INFO,
                """
                        IndexSelectionVerdict:
                          indexUsedByWhereClause = %s
                          selectedIndex          = %s
                          isUniqueIndex          = %s
                        """.formatted(
                        indexSelectionVerdict.indexIsUsedGivenWhereClause(),
                        indexSelectionVerdict.index(),
                        indexSelectionVerdict.index() == null ? "N/A" : indexSelectionVerdict.index()
                )
        );

        // build projection
        int[] projectionColumns = new int[queryTree.projections.size()];
        int idxCol = 0;

        for (var column : queryTree.projections) {
            projectionColumns[idxCol] = column.getColumnPosition();

            LOGGER.log(INFO,
                    "Projection column[%d]: name=%s position=%d table=%s"
                            .formatted(
                                    idxCol,
                                    column.columnName,
                                    column.getColumnPosition(),
                                    column.table.name
                            )
            );

            idxCol++;
        }

        int entrySize = calculateQueryResultEntrySize(
                tb.schema(),
                queryTree.projections.size(),
                projectionColumns
        );

        LOGGER.log(INFO,
                """
                        Computed entry size:
                          entrySize = %d
                        """.formatted(entrySize)
        );

        if (indexSelectionVerdict.indexIsUsedGivenWhereClause()) {

            LOGGER.log(INFO,
                    """
                            PLAN DECISION → IndexScan
                              index        = %s
                              projection   = %s
                              entrySize    = %d
                            """.formatted(
                            indexSelectionVerdict.index(),
                            Arrays.toString(projectionColumns),
                            entrySize
                    )
            );

            // return the index scan with projection
            return new IndexScan(
                    indexSelectionVerdict.index(),
                    projectionColumns,
                    entrySize
            );

        } else {

            LOGGER.log(INFO,
                    """
                            PLAN DECISION → FullScan (PK index)
                              pkIndex     = %s
                              projection  = %s
                              entrySize   = %d
                            """.formatted(
                            tb.primaryKeyIndex(),
                            Arrays.toString(projectionColumns),
                            entrySize
                    )
            );

            // then must get the PK index, ScanWithProjection
            return new FullScan(
                    tb.primaryKeyIndex(),
                    projectionColumns,
                    entrySize
            );
        }
    }

    private static int calculateQueryResultEntrySize(Schema schema, int nProj, int[] projectionColumns) {
        int entrySize = 0;
        for (int i = 0; i < nProj; i++) {
            entrySize += schema.columnDataType(projectionColumns[i]).value;
        }
        return entrySize;
    }

    private IndexSelectionVerdict getOptimalIndex(final Table table, List<WherePredicate> wherePredicates) {


        LOGGER.log(INFO,
                """
                        >>> ENTER getOptimalIndex(verdict)
                          table.name              = %s
                          wherePredicates.size    = %s
                          secondaryIndexMap.size  = %d
                          partialIndexMap.size    = %d
                          pk.key                  = %s
                        """.formatted(
                        table == null ? "null" : table.name,
                        wherePredicates == null ? "null" : wherePredicates.size(),
                        table == null || table.secondaryIndexMap == null ? -1 : table.secondaryIndexMap.size(),
                        table == null || table.partialIndexMap == null ? -1 : table.partialIndexMap.size(),
                        table == null ? "null" : table.underlyingPrimaryKeyIndex().key()
                )
        );

        // log predicates one-by-one (optional but very useful)
        if (wherePredicates != null) {
            for (int i = 0; i < wherePredicates.size(); i++) {
                WherePredicate wp = wherePredicates.get(i);
                LOGGER.log(INFO,
                        "  wherePredicates[%d]: table=%s colPos=%d expr=%s"
                                .formatted(
                                        i,
                                        wp == null || wp.columnReference == null || wp.columnReference.table == null
                                                ? "null"
                                                : wp.columnReference.table.name,
                                        wp == null ? -1 : wp.getColumnPosition(),
                                        wp == null ? "null" : wp.expression
                                )
                );
            }
        }

        LOGGER.log(INFO, "Building IntStream for equality predicates on table=" + (table == null ? "null" : table.name));

        final IntStream intStream = wherePredicates.stream()
                .filter(wherePredicate -> {
                    boolean eq = ExpressionTypeEnum.equality(wherePredicate.expression);
                    boolean sameTable = wherePredicate.columnReference.table.equals(table);
                    boolean keep = eq && sameTable;

                    LOGGER.log(INFO,
                            "  filter-eval: expr=%s eq=%s predTable=%s sameTable=%s -> keep=%s"
                                    .formatted(
                                            wherePredicate.expression,
                                            eq,
                                            wherePredicate.columnReference.table == null ? "null" : wherePredicate.columnReference.table.name,
                                            sameTable,
                                            keep
                                    )
                    );

                    return keep;
                })
                .mapToInt(WherePredicate::getColumnPosition);

        LOGGER.log(INFO, "Converting IntStream to array...");

        final int[] columnsForIndexSelection = intStream.toArray();

        LOGGER.log(INFO,
                "columnsForIndexSelection: len=%d cols=%s"
                        .formatted(columnsForIndexSelection.length, Arrays.toString(columnsForIndexSelection))
        );

        final IIndexKey indexKey = KeyUtils.buildIndexKey(columnsForIndexSelection);

        LOGGER.log(INFO, "built indexKey=" + indexKey);

        boolean pkMatch = table.underlyingPrimaryKeyIndex().key().equals(indexKey);
        LOGGER.log(INFO, "fastPath(PK): pk.key.equals(indexKey) = " + pkMatch);

        // fast path (1): all columns are part of the primary index
        if (pkMatch) {
            LOGGER.log(INFO, "FAST PATH HIT: PK INDEX");
            return new IndexSelectionVerdict(true, table.primaryKeyIndex(), columnsForIndexSelection);
        }

        boolean secondaryMatch = table.secondaryIndexMap.containsKey(indexKey);
        LOGGER.log(INFO, "fastPath(secondary): secondaryIndexMap.containsKey(indexKey) = " + secondaryMatch);

        // fast path (2): all columns are part of a secondary index
        if (secondaryMatch) {
            LOGGER.log(INFO, "FAST PATH HIT: SECONDARY INDEX -> " + table.secondaryIndexMap.get(indexKey));
            return new IndexSelectionVerdict(
                    true,
                    table.secondaryIndexMap.get(indexKey),
                    columnsForIndexSelection);
        }

        boolean partialMatch = table.partialIndexMap.containsKey(indexKey);
        LOGGER.log(INFO, "fastPath(partial): partialIndexMap.containsKey(indexKey) = " + partialMatch);

        if (partialMatch) {
            LOGGER.log(INFO, "FAST PATH HIT: PARTIAL INDEX -> " + table.partialIndexMap.get(indexKey));
            return new IndexSelectionVerdict(
                    true,
                    table.partialIndexMap.get(indexKey),
                    columnsForIndexSelection);
        }

        LOGGER.log(INFO,
                "No exact index match, calling subset getOptimalIndex(table, columnsForIndexSelection) with cols="
                        + Arrays.toString(columnsForIndexSelection));

        final ReadWriteIndex<IKey> indexSelected = this.getOptimalIndex(table, columnsForIndexSelection);

        LOGGER.log(INFO, "Returned from subset getOptimalIndex: indexSelected=" + indexSelected);

        // is the index completely covered by the columns in the filter?
        if (indexSelected == null) {
            LOGGER.log(INFO, "indexSelected == null -> returning verdict(false, pkIndex)");
            // then just select the Primary index
            return new IndexSelectionVerdict(false, table.primaryKeyIndex(), columnsForIndexSelection);
        }

        LOGGER.log(INFO, "Computing filterColumns (columns not contained in selected index) ...");

        // columns not in the index, but require filtering
        final IntStream filteredStream = Arrays.stream(columnsForIndexSelection)
                .filter(w -> {
                    boolean keep = !indexSelected.containsColumn(w);
                    LOGGER.log(INFO, "  filterColumns-eval: col=" + w + " indexContains=" + !keep + " -> keep=" + keep);
                    return keep;
                });

        final int[] filterColumns = filteredStream.toArray();

        LOGGER.log(INFO,
                "filterColumns computed: len=%d cols=%s"
                        .formatted(filterColumns.length, Arrays.toString(filterColumns))
        );

        boolean chooseSecondary = table.secondaryIndexMap.containsKey(indexSelected.key());
        LOGGER.log(INFO,
                "final choice: secondaryIndexMap.containsKey(indexSelected.key()) = "
                        + chooseSecondary
                        + " (indexSelected.key()=" + indexSelected.key() + ")"
        );

        IMultiVersionIndex chosen =
                chooseSecondary
                        ? table.secondaryIndexMap.get(indexSelected.key())
                        : table.partialIndexMap.get(indexSelected.key());

        LOGGER.log(INFO, "Returning verdict(true, chosen=" + chosen + ", filterColumns=" + Arrays.toString(filterColumns) + ")");

        return new IndexSelectionVerdict(true,
                chosen,
                filterColumns);
    }

    private ReadWriteIndex<IKey> getOptimalIndex(Table table, int[] filterColumns) {

        LOGGER.log(INFO, "subset getOptimalIndex: filterColumns.length=" + filterColumns.length
                + " filterColumns=" + Arrays.toString(filterColumns));

        if (filterColumns.length == 0) {
            LOGGER.log(INFO, "subset getOptimalIndex: empty filterColumns -> returning null (no subset index search)");
            return null;
        }

        final long t0 = System.nanoTime();

        LOGGER.log(INFO,
                """
                        >>> ENTER getOptimalIndex(subset)
                          table.name           = %s
                          filterColumns.length = %d
                          filterColumns        = %s
                          secondaryIndexMap.size = %d
                        """.formatted(
                        table == null ? "null" : table.name,
                        filterColumns == null ? -1 : filterColumns.length,
                        filterColumns == null ? "null" : Arrays.toString(filterColumns),
                        table == null || table.secondaryIndexMap == null ? -1 : table.secondaryIndexMap.size()
                )
        );

        LOGGER.log(INFO,
                "Calling Combinatorics.getAllPossibleColumnCombinations(filterColumns) ... elapsed(ms)="
                        + ((System.nanoTime() - t0) / 1_000_000.0));

        List<int[]> combinations = Combinatorics.getAllPossibleColumnCombinations(filterColumns);

        LOGGER.log(INFO,
                "Returned from Combinatorics: combinations.size=%s elapsed(ms)=%.3f"
                        .formatted(
                                combinations == null ? "null" : combinations.size(),
                                (System.nanoTime() - t0) / 1_000_000.0
                        )
        );

        // heuristic: return the one that embraces more columns
        ReadWriteIndex<IKey> bestSoFar = null;
        int maxLength = 0;

        int i = 0;
        for (int[] arr : combinations) {

            // progress marker every 1000 combinations (so you see if it's looping forever)
            if (i % 1000 == 0) {
                LOGGER.log(INFO,
                        "subset loop progress: i=%d maxLength=%d elapsed(ms)=%.3f"
                                .formatted(i, maxLength, (System.nanoTime() - t0) / 1_000_000.0));
            }

            IKey indexKey = KeyUtils.buildIndexKey(arr);

            var mvIdx = table.secondaryIndexMap.get(indexKey);

            if (mvIdx != null) {
                LOGGER.log(INFO,
                        "FOUND secondary index candidate: key=%s arr=%s arr.length=%d"
                                .formatted(indexKey, Arrays.toString(arr), arr.length));

                if (arr.length > maxLength) {
                    bestSoFar = mvIdx.getUnderlyingIndex();
                    maxLength = arr.length;

                    LOGGER.log(INFO,
                            "UPDATED bestSoFar: maxLength=%d bestSoFar=%s"
                                    .formatted(maxLength, bestSoFar));
                }
            }

            i++;
        }

        LOGGER.log(INFO,
                """
                        >>> EXIT getOptimalIndex(subset)
                          bestSoFar = %s
                          maxLength = %d
                          elapsed(ms)=%.3f
                        """.formatted(
                        bestSoFar,
                        maxLength,
                        (System.nanoTime() - t0) / 1_000_000.0
                )
        );

        return bestSoFar;
    }

}