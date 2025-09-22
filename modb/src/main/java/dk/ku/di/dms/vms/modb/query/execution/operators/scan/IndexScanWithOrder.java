package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class IndexScanWithOrder extends AbstractScanWithOrder {

    public IndexScanWithOrder(IMultiVersionIndex index, int[] projectionColumns, int orderByColumn, int entrySize) {
        super(index, projectionColumns, orderByColumn, entrySize);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            this.insert(result, iterator.next());
        }
        return this.projectIfNecessary(result);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys) {
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            this.insert(result, iterator.next());
        }
        return this.projectIfNecessary(result);
    }

    @Override
    public boolean isIndexScanWithOrder() {
        return true;
    }

    @Override
    public IndexScanWithOrder asIndexScanWithOrder() {
        return this;
    }

}