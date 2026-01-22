package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.List;

public final class VModbTableAccessRule extends RelOptRule {

    public static final VModbTableAccessRule INSTANCE = new VModbTableAccessRule();

    private VModbTableAccessRule() {
        super(operand(LogicalTableScan.class, any()), "VModbTableScanRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableScan scan = call.rel(0);

        if (scan.getTraitSet().getConvention() == VModbConvention.INSTANCE) return;

        List<String> qualified = scan.getTable().getQualifiedName();
        String schema = qualified.size() >= 2 ? qualified.get(qualified.size() - 2) : "default";

        RelNode out = VModbTableAccess.create(
                scan.getCluster(),
                scan.getTable(),
                schema,
                null,  // projects = ALL
                null,  // filter = null
                null,
                null
        );

        if (out.getTraitSet().getConvention() != VModbConvention.INSTANCE) {
            out = out.copy(
                    out.getTraitSet().replace(VModbConvention.INSTANCE),
                    out.getInputs()
            );
        }

        call.transformTo(out);
    }
}