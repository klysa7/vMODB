package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FullScanWithOrder extends AbstractScanWithOrder {

    public FullScanWithOrder(IMultiVersionIndex index,
                             int[] projectionColumns,
                             int orderByColumn,
                             int entrySize) {
        super(index, projectionColumns, orderByColumn, entrySize);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx){
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()){
            this.insert(result, iterator.next());
        }
        return this.projectIfNecessary(result);
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, FilterContext filterContext){
        List<Object[]> result = new ArrayList<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()){
            Object[] record = iterator.next();
            if(this.index.checkCondition(filterContext, record)) {
                this.insert(result, record);
            }
        }
        return this.projectIfNecessary(result);
    }

    @Override
    public boolean isFullScanWithOrder() {
        return true;
    }

    @Override
    public FullScanWithOrder asFullScanWithOrder() {
        return this;
    }

}
