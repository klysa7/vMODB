package dk.ku.di.dms.vms.calcite.modb.runtime;

import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

public interface VModbRuntime {
    TransactionContext tx();
    IMultiVersionIndex index(String schema, String table);
    int entrySize(String schema, String table);
}