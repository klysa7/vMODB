package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops;

public final class ScanAllOperation implements SubPlanOperation {
    public final String schema;
    public final String table;

    public ScanAllOperation(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    @Override public String toString() {
        return "ScanAll(" + schema + "." + table + ")";
    }
}