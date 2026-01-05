package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public final class VModbTableAccessRule extends RelOptRule {

    public static final VModbTableAccessRule INSTANCE = new VModbTableAccessRule();

    private VModbTableAccessRule() {
        super(
                operand(LogicalProject.class,
                        operand(LogicalFilter.class,
                                operand(LogicalTableScan.class, none())
                        )
                ),
                "VModbTableAccessRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalFilter filter = call.rel(1);
        LogicalTableScan scan = call.rel(2);

        List<String> qualifiedNames = scan.getTable().getQualifiedName();
        String schema = qualifiedNames.size() >= 2 ? qualifiedNames.get(qualifiedNames.size() - 2) : "default";

        int[] projects = project.getProjects()
                .stream()
                .mapToInt(VModbTableAccessRule::requireInputRef)
                .toArray();

        FilterContext filterCtx = RexToVModb.toFilterContext(filter.getCondition());

        VModbTableAccess vModbTableAccess = VModbTableAccess.create(scan.getCluster(), scan.getTable(),
                schema, projects, filterCtx, null, null);

        call.transformTo(vModbTableAccess);
    }

    private static int requireInputRef(RexNode expr) {
        if (expr instanceof RexInputRef ref) {
            return ref.getIndex();
        }
        throw new UnsupportedOperationException(
                "Simple column projections supported: " + expr
        );
    }
}