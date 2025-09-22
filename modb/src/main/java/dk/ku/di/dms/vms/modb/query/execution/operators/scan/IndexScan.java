package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * On-flight scanning, filtering, and projection in a single operator.
 * The caller knows the output format.
 * But called does not know how many records are.
 * Header:
 * int - number of rows returned
 * This must be performed by a proper method at the end of the procedure
 * We can have a method called seal or close() in abstract operator
 * It will put the information on header.
 */
public final class IndexScan extends AbstractScan {

    public IndexScan(IMultiVersionIndex index, int[] projectionColumns, int entrySize) {
        super(entrySize, index, projectionColumns);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey key) {
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, key);
        while(iterator.hasNext()){
            res.add(this.getProjection(iterator.next()));
        }
        return res;
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, IKey[] keys) {
        List<Object[]> res = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx, keys);
        while(iterator.hasNext()){
            res.add(this.getProjection(iterator.next()));
        }
        return res;
    }

    @Override
    public boolean isIndexScan() {
        return true;
    }

    @Override
    public IndexScan asIndexScan() {
        return this;
    }

}