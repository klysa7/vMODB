package dk.ku.di.dms.vms.calcite.legendary.translate;

import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;

import java.util.ArrayList;

public final class FilterBinder {

//    private FilterBinder() {}
//
//    public static FilterContext bind(FilterContext ctx, Object[] params) {
//        if (ctx == null) return null;
//
//        FilterContext bound = new FilterContext();
//        bound.filterTypes = ctx.filterTypes;
//        bound.filterColumns = ctx.filterColumns;
//        bound.biPredicates = ctx.biPredicates;
//        bound.predicates = ctx.predicates;
//        bound.biPredicateParams = new ArrayList<>(ctx.biPredicateParams.size());
//        for (Object p : ctx.biPredicateParams) {
//            if (p instanceof RexToVModb.ParamRef ref) {
//                bound.biPredicateParams.add(params[ref.index()]);
//            } else {
//                bound.biPredicateParams.add(p);
//            }
//        }
//        return bound;
//    }
}
