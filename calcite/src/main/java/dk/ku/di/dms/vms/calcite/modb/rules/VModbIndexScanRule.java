package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.convention.VModbConvention;
import dk.ku.di.dms.vms.calcite.modb.rel.VModbIndexScan;
import dk.ku.di.dms.vms.calcite.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

public final class VModbIndexScanRule extends ConverterRule {

    public static final VModbIndexScanRule INSTANCE = new VModbIndexScanRule();

    private VModbIndexScanRule() {
        super(LogicalFilter.class, Convention.NONE, VModbConvention.INSTANCE, "VModbIndexScanRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;

        if (!(filter.getInput() instanceof LogicalTableScan scan)) return null;

        KeyPredicate kp = findSinglePkEquality(filter.getCondition());
        if (kp == null) return null;
        FilterContext filterCtx = RexToVModb.toFilterContext(filter.getCondition());

        var qualifiedName = scan.getTable().getQualifiedName();
        String schema = qualifiedName.size() >= 2 ? qualifiedName.get(qualifiedName.size() - 2) : "default";

        IKey key = buildKeyForPk(scan, kp);
        if (key == null) return null;

        return VModbIndexScan.create(scan.getCluster(),
                scan.getTable(), schema, null, filterCtx, key);
    }

    private record KeyPredicate(int pkColumnIndex, Object literalOrParam) {}

    private static KeyPredicate findSinglePkEquality(RexNode cond) {
        if (cond instanceof RexCall call && call.getKind() == SqlKind.AND) {
            for (RexNode op : call.operands) {
                KeyPredicate kp = findSinglePkEquality(op);
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


    private static IKey buildKeyForPk(LogicalTableScan scan, KeyPredicate kp) {
        // todo need to be implemented
        return null;
    }
}
