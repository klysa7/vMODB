package dk.ku.di.dms.vms.calcite.legendary.runtime;

import dk.ku.di.dms.vms.modb.transaction.TransactionContext;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;

public interface VModbRuntime {
    TransactionContext transactionContext();
    IMultiVersionIndex index(String schema, String table);
    Integer entrySize(String schema, String table);
}