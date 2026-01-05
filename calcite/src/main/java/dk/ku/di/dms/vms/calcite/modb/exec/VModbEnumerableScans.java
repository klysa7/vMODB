package dk.ku.di.dms.vms.calcite.modb.exec;

import dk.ku.di.dms.vms.calcite.modb.runtime.VModbKeys;
import dk.ku.di.dms.vms.calcite.modb.runtime.VModbRuntime;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScan;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;

import java.util.List;

public final class VModbEnumerableScans {

    private VModbEnumerableScans() {}

    public static Enumerable<Object[]> fullScan(DataContext ctx,
                                                String schema,
                                                String table,
                                                int[] projects,
                                                int fieldCount) {

        Object rt = ctx.get(VModbKeys.RUNTIME);
        if (!(rt instanceof VModbRuntime runtime)) {
            throw new IllegalStateException("Missing VModbRuntime under key: " + VModbKeys.RUNTIME);
        }

        if (schema == null || schema.isBlank()) schema = "default";

        var index = runtime.index(schema, table);
        int entrySize = runtime.entrySize(schema, table);

        // Your AbstractScan.getProjection needs non-null projectionColumns:
        int[] projectionColumns = (projects != null) ? projects : identityProjection(fieldCount);

        // ✅ HERE is FullScan usage:
        FullScan scan = new FullScan(index, projectionColumns, entrySize);
        List<Object[]> rows = scan.runAsEmbedded(runtime.tx());

        return Linq4j.asEnumerable(rows);
    }

    private static int[] identityProjection(int n) {
        int[] p = new int[n];
        for (int i = 0; i < n; i++) p[i] = i;
        return p;
    }
}