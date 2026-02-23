package dk.ku.di.dms.vms.calcite.legacy;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.legacy.runtime.VModbRuntime;
import dk.ku.di.dms.vms.calcite.legacy.translate.RexToVModb;
import dk.ku.di.dms.vms.modb.query.execution.filter.FilterContext;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.FullScan;
import dk.ku.di.dms.vms.modb.query.execution.operators.scan.IndexScan;

import java.util.ArrayList;
import java.util.List;

public final class VModbExecutor {

    private final VModbRuntime runtime;

    public VModbExecutor(VModbRuntime runtime) {
        this.runtime = runtime;
    }


    public List<Object[]> execute(VModbTableAccess plan, Object[] params) {
        var index = runtime.index(plan.schemaName, plan.tableName);
        int entrySize = runtime.entrySize(plan.schemaName, plan.tableName);

        FilterContext boundFilter = bindParams(null, params);

        if (plan.key != null || plan.keys != null) {
            IndexScan scan = new IndexScan(index, plan.projects, entrySize);

            if (plan.key != null) {
                return (boundFilter == null)
                        ? scan.runAsEmbedded(runtime.transactionContext(), plan.key)
                        : scan.runAsEmbedded(runtime.transactionContext(), plan.key, boundFilter);
            } else {
                return scan.runAsEmbedded(runtime.transactionContext(), plan.keys);
            }

        } else {
            FullScan scan = new FullScan(index, plan.projects, entrySize);
            return (boundFilter == null)
                    ? scan.runAsEmbedded(runtime.transactionContext())
                    : scan.runAsEmbedded(runtime.transactionContext(), boundFilter);
        }
    }


    private static FilterContext bindParams(FilterContext ctx, Object[] params) {
        if (ctx == null) return null;
        if (params == null) params = new Object[0];

        FilterContext bound = new FilterContext();
        bound.filterTypes = ctx.filterTypes;
        bound.filterColumns = ctx.filterColumns;
        bound.biPredicates = ctx.biPredicates;
        bound.predicates = ctx.predicates;

        bound.biPredicateParams = new ArrayList<>(ctx.biPredicateParams.size());
        for (Object p : ctx.biPredicateParams) {
            if (p instanceof RexToVModb.ParamRef ref) {
                int i = ref.index();
                if (i < 0 || i >= params.length) {
                    throw new IllegalArgumentException("Missing value for ?" + i);
                }
                bound.biPredicateParams.add(params[i]);
            } else {
                bound.biPredicateParams.add(p);
            }
        }
        return bound;
    }
}