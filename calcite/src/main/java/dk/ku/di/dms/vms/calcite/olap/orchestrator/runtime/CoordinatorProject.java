package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import java.util.ArrayList;
import java.util.List;

public final class CoordinatorProject {

    private static final System.Logger LOGGER =
            System.getLogger(CoordinatorProject.class.getName());

    public PushdownResponse project(PushdownResponse input, int[] indices) {

        List<String> projectColumns = new ArrayList<>(indices.length);
        for (int idx : indices) {
            if (idx < 0 || idx >= input.width()) {
                throw new IllegalArgumentException("Project index " + idx + " out of range width=" + input.width());
            }
            projectColumns.add(input.columns.get(idx));
        }

        List<List<Object>> outRows = new ArrayList<>(input.rows.size());
        input.rows.forEach(row -> {
            List<Object> out = new ArrayList<>(indices.length);
            for (int idx : indices) {
                out.add(row.get(idx));
            }
            outRows.add(out);
        });

        return new PushdownResponse("coordinator", input.snapshot, projectColumns, outRows);
    }
}