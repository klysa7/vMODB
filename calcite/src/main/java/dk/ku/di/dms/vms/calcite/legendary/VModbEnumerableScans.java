package dk.ku.di.dms.vms.calcite.legendary;

import dk.ku.di.dms.vms.calcite.legendary.runtime.VModbKeys;
import dk.ku.di.dms.vms.calcite.legendary.runtime.VModbRuntime;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScan;
import dk.ku.di.dms.vms.modb.transaction.multiversion.index.IMultiVersionIndex;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.List;

public final class VModbEnumerableScans {

    private VModbEnumerableScans() {}

    public static Enumerable<Object[]> fullScan(DataContext dataContext,
                                                String schema,
                                                String table,
                                                int[] projects,
                                                Integer fieldCount) {

        Object rt = dataContext.get(VModbKeys.RUNTIME);
        if (!(rt instanceof VModbRuntime runtime)) {
            throw new IllegalStateException();
        }

        if (schema == null || schema.isBlank()) schema = "default";

        IMultiVersionIndex index = runtime.index(schema, table);
        Integer entrySize = runtime.entrySize(schema, table);

        int[] projectionColumns = (projects != null) ? projects : identityProjection(fieldCount);

        FullScan scan = new FullScan(index, projectionColumns, entrySize);
        List<Object[]> rows = scan.runAsEmbedded(runtime.transactionContext());

        return Linq4j.asEnumerable(rows);
    }

    private static int[] identityProjection(Integer fieldCount) {
        int[] projection = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) projection[i] = i;
        return projection;
    }
}