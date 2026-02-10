package dk.ku.di.dms.vms.calcite.legacy;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;

public final class VModbScanRule extends RelOptRule {

    public static final VModbScanRule INSTANCE = new VModbScanRule();

    private VModbScanRule() {
        super(operand(LogicalTableScan.class, any()), "VModbScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);

        var qualifiedName = scan.getTable().getQualifiedName();
        String schema = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "default";

        call.transformTo(VModbScan.create(scan.getCluster(), scan.getTable(), schema, null));
    }
}