package dk.ku.di.dms.vms.calcite.legendary.translate;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterType;
import dk.ku.di.dms.vms.modb.query.execution.filter.types.TypedBiPredicate;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

public final class RexToVModb {

    private RexToVModb() {}

    public record ParamRef(int index) {}

    public static FilterContext toFilterContext(RexNode condition) {
        if (condition == null) return null;

        FilterContext ctx = new FilterContext();
        ctx.filterTypes = new ArrayList<>();
        ctx.filterColumns = new ArrayList<>();
        ctx.biPredicates = new ArrayList<>();
        ctx.biPredicateParams = new ArrayList<>();
        ctx.predicates = new ArrayList<>();

        List<RexNode> conjuncts = new ArrayList<>();
        flattenAnd(condition, conjuncts);

        for (RexNode c : conjuncts) {
            if (!(c instanceof RexCall call)) {
                throw new UnsupportedOperationException("Unsupported");
            }
            if (call.getKind() != SqlKind.EQUALS) {
                throw new UnsupportedOperationException();
            }

            RexNode left = call.operands.get(0);
            RexNode right = call.operands.get(1);

            RexInputRef col;
            Object paramOrLiteral;

            if (left instanceof RexInputRef lcol) {
                col = lcol;
                paramOrLiteral = extractLiteralOrParam(right);
            } else if (right instanceof RexInputRef rcol) {
                col = rcol;
                paramOrLiteral = extractLiteralOrParam(left);
            } else {
                throw new UnsupportedOperationException();
            }

            ctx.filterTypes.add(FilterType.BP);
            ctx.filterColumns.add(col.getIndex());

            @SuppressWarnings("rawtypes")
            TypedBiPredicate eq = (a, b) -> a != null && a.equals(b);

            ctx.biPredicates.add(eq);
            ctx.biPredicateParams.add(paramOrLiteral);
        }

        return ctx;
    }

    private static Object extractLiteralOrParam(RexNode node) {
        if (node instanceof RexLiteral lit) {
            return lit.getValueAs(Object.class);
        }
        if (node instanceof RexDynamicParam p) {
            return new ParamRef(p.getIndex());
        }
        throw new UnsupportedOperationException();
    }

    private static void flattenAnd(RexNode node, List<RexNode> out) {
        if (node instanceof RexCall call && call.getKind() == SqlKind.AND) {
            for (RexNode op : call.operands) flattenAnd(op, out);
        } else {
            out.add(node);
        }
    }
}
