package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import java.util.List;
/** Result of distributed execution */
public final class PushdownResponse {
    public final String source;
    public final Long snapshot;
    public final List<String> columns;
    public final List<List<Object>> rows;

    public PushdownResponse(String source, Long snapshot, List<String> columns,
                            List<List<Object>> rows) {
        this.source = source;
        this.snapshot = snapshot;
        this.columns = columns;
        this.rows = rows;
    }

    public int width() { return columns.size(); }
    public int size() { return rows.size(); }
}