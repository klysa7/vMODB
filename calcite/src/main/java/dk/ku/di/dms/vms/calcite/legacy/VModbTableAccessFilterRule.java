package dk.ku.di.dms.vms.calcite.legacy;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.legacy.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;

import java.util.List;

public final class VModbTableAccessFilterRule extends RelOptRule {

    public static final VModbTableAccessFilterRule INSTANCE = new VModbTableAccessFilterRule();

    private VModbTableAccessFilterRule() {
        super(operand(LogicalFilter.class,
                operand(LogicalTableScan.class, none())), "VModbTableAccessFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalTableScan scan = call.rel(1);

        List<String> qualifiedName = scan.getTable().getQualifiedName();
        String schema = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "default";

        FilterContext filterCtx = RexToVModb.toFilterContext(filter.getCondition());

        call.transformTo(VModbTableAccess.create(scan.getCluster(), scan.getTable(),
                schema, null, filterCtx, null, null));
    }
}