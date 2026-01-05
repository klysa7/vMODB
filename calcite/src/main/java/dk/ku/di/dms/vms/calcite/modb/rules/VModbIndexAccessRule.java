package dk.ku.di.dms.vms.calcite.modb.rules;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbIndexScan;
import dk.ku.di.dms.vms.calcite.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.definition.key.IKey;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

public final class VModbIndexAccessRule extends RelOptRule {

    public static final VModbIndexAccessRule INSTANCE = new VModbIndexAccessRule();

    private VModbIndexAccessRule() {
        super(
                operand(LogicalProject.class,
                        operand(LogicalFilter.class,
                                operand(LogicalTableScan.class, none())
                        )
                ),
                "VModbIndexAccessRule"
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalFilter filter = call.rel(1);
        LogicalTableScan scan = call.rel(2);

        // 1) detect pk equality inside filter
        KeyPredicate kp = findPkEquality(filter.getCondition());
        if (kp == null) return;

        // 2) build vMODB filter context (same as TableAccess)
        FilterContext filterCtx = RexToVModb.toFilterContext(filter.getCondition());

        // 3) schema name
        var qn = scan.getTable().getQualifiedName();
        String schema = qn.size() >= 2 ? qn.get(qn.size() - 2) : "default";

        // 4) projection columns (same restriction you had: RexInputRef only)
        int[] projects = project.getProjects().stream().mapToInt(p -> {
            if (!(p instanceof RexInputRef ref)) {
                throw new UnsupportedOperationException("Only simple column projections supported: " + p);
            }
            return ref.getIndex();
        }).toArray();

        // 5) build IKey (THIS MUST NOT BE NULL)
        IKey key = buildKeyForPk(scan, kp);
        if (key == null) return;

        call.transformTo(
                VModbIndexScan.create(
                        scan.getCluster(),
                        scan.getTable(),
                        schema,
                        projects,
                        filterCtx,
                        key
                )
        );
    }

    /** detected: pkCol = (? or literal) */
    private record KeyPredicate(int colIndex, Object litOrParam) {}

    private static KeyPredicate findPkEquality(RexNode cond) {
        // allow AND, pick first pk equality we find
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

    /**
     * TODO: Implement this using your vMODB schema metadata.
     * It must:
     *   - verify kp.colIndex is a primary key column
     *   - create an IKey for that pk value
     */
    private static IKey buildKeyForPk(LogicalTableScan scan, KeyPredicate kp) {
        // --- You must plug in your table metadata -> schema -> pk columns -> key builder ---
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
