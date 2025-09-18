package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;

public final class IndexScanWithOrder extends AbstractScan {

    private final int orderByColumn;

    public IndexScanWithOrder(IMultiVersionIndex index, int[] projectionColumns, int orderByColumn, int entrySize) {
        super(entrySize, index, projectionColumns);
        this.orderByColumn = orderByColumn;
    }

    @Override
    public boolean isIndexScanWithOrder() {
        return true;
    }

    @Override
    public IndexScanWithOrder asIndexScanWithOrder() {
        return this;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key) {
        Map<Object, Object[]> orderedMap = new TreeMap<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            var next = iterator.next();
            orderedMap.put(next[this.orderByColumn], next);
        }
        return orderedMap.values().stream().toList();
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys) {
        Map<Object, Object[]> orderedMap = new TreeMap<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            var next = iterator.next();
            orderedMap.put(next[this.orderByColumn], next);
        }
        return orderedMap.values().stream().toList();
    }

}