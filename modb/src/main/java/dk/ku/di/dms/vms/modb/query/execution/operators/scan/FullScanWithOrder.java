package dk.ku.di.dms.vms.modb.query.execution.operators.scan;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

import java.util.*;

public class FullScanWithOrder extends AbstractScan {

    private final int orderByColumn;

    public FullScanWithOrder(IMultiVersionIndex index,
                             int[] projectionColumns,
                             int orderByColumn,
                             int entrySize) {
        super(entrySize, index, projectionColumns);
        this.orderByColumn = orderByColumn;
    }

//    public MemoryRefNode run(FilterContext filterContext){
//        Iterator<IKey> iterator = index.iterator();
//        while(iterator.hasNext()){
//            if(index.checkCondition(iterator, filterContext)){
//                append(iterator, projectionColumns);
//            }
//            iterator.next();
//        }
//        return memoryRefNode;
//    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx){
        Map<Object, Object[]> orderedMap = new TreeMap<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()){
            var next = iterator.next();
            orderedMap.put(next[this.orderByColumn], next);
        }
        return orderedMap.values().stream().toList();
    }

    public List<Object[]> runAsEmbedded(TransactionContext txCtx, FilterContext filterContext){
        Map<Object, Object[]> orderedMap = new TreeMap<>();
        Iterator<Object[]> iterator = this.index.iterator(txCtx);
        while(iterator.hasNext()){
            Object[] obj = iterator.next();
            if(this.index.checkCondition(filterContext, obj)) {
                // ideally this should be a insertion sort. equal order column would remove the previous element
                orderedMap.put(obj[this.orderByColumn], obj);
            }
        }
        return orderedMap.values().stream().toList();
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
