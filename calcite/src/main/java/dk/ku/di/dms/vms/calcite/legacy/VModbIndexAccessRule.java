package dk.ku.di.dms.vms.calcite.legacy;

import dk.ku.di.dms.vms.calcite.legacy.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public final class VModbIndexAccessRule extends RelOptRule {

    public static final VModbIndexAccessRule INSTANCE = new VModbIndexAccessRule();

    private VModbIndexAccessRule() {
        super(operand(LogicalProject.class, operand(LogicalFilter.class,
                operand(LogicalTableScan.class, none()))), "VModbIndexAccessRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalFilter filter = call.rel(1);
        LogicalTableScan scan = call.rel(2);

        KeyPredicate keyPredicate = findPkEquality(filter.getCondition());
        if (keyPredicate == null) return;
        FilterContext filterCtx = RexToVModb.toFilterContext(filter.getCondition());
        List<String> qualifiedNames = scan.getTable().getQualifiedName();
        String schema = qualifiedNames.size() >= 2 ? qualifiedNames.get(qualifiedNames.size() - 2) : "default";

        int[] projects = project.getProjects().stream().mapToInt(p -> {
            if (!(p instanceof RexInputRef ref)) {
                throw new UnsupportedOperationException("Only simple column projections support");
            }
            return ref.getIndex();
        }).toArray();

        IKey key = buildKeyForPk(scan, keyPredicate);
        if (key == null) return;

        call.transformTo(VModbIndexScan.create(scan.getCluster(), scan.getTable(), schema, projects, filterCtx, key));
    }

    private record KeyPredicate(int colIndex, Object litOrParam) {}

    private static KeyPredicate findPkEquality(RexNode cond) {
        if (cond instanceof RexCall call && call.getKind() == SqlKind.AND) {
            for (RexNode op : call.operands) {
                KeyPredicate kp = findPkEquality(op);
                if (kp != null) return kp;
            }
            return null;
        }

        if (!(cond instanceof RexCall eq) || eq.getKind() != SqlKind.EQUALS) return null;
        RexNode a = eq.operands.get(0);
        RexNode b = eq.operands.get(1);

        if (a instanceof RexInputRef col) {
            Object rhs = literalOrParam(b);
            return rhs == null ? null : new KeyPredicate(col.getIndex(), rhs);
        }
        if (b instanceof RexInputRef col) {
            Object lhs = literalOrParam(a);
            return lhs == null ? null : new KeyPredicate(col.getIndex(), lhs);
        }
        return null;
    }

    private static Object literalOrParam(RexNode n) {
        if (n instanceof RexLiteral lit) return lit.getValueAs(Object.class);
        if (n instanceof RexDynamicParam p) return new RexToVModb.ParamRef(p.getIndex());
        return null;
    }

//TODO: Implement this using your vMODB schema metadata.
    private static IKey buildKeyForPk(LogicalTableScan scan, KeyPredicate kp) {
        // Example shape (you'll adapt):
        // VModbTable t = scan.getTable().unwrap(VModbTable.class);
        // Schema s = t.schema();
        // if (!isPkColumn(s, kp.colIndex)) return null;
        // Object[] record = new Object[s.numColumns()];
        // record[kp.colIndex] = kp.litOrParam;
        // return KeyUtils.buildRecordKey(s.getPrimaryKeyColumns(), record);
        return null;
    }
}
