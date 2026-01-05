package dk.ku.di.dms.vms.calcite.modb.exec;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.modb.runtime.VModbRuntime;
import dk.ku.di.dms.vms.calcite.translate.RexToVModb; // <-- IMPORTANT: same package as RexToVModb above
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

    /**
     * Execute a vMODB physical scan node.
     *
     * @param plan   vMODB physical plan (currently only VModbTableAccess supported)
     * @param params values for ?0, ?1, ... (in Calcite order)
     */
    public List<Object[]> execute(VModbTableAccess plan, Object[] params) {
        var index = runtime.index(plan.schemaName, plan.tableName);
        int entrySize = runtime.entrySize(plan.schemaName, plan.tableName);

        // Bind ?0 placeholders to actual parameter values
        FilterContext boundFilter = bindParams(plan.filter, params);

        // Decide which operator to use:
        if (plan.key != null || plan.keys != null) {
            IndexScan scan = new IndexScan(index, plan.projects, entrySize);

            if (plan.key != null) {
                return (boundFilter == null)
                        ? scan.runAsEmbedded(runtime.tx(), plan.key)
                        : scan.runAsEmbedded(runtime.tx(), plan.key, boundFilter);
            } else {
                // NOTE: you don’t have IndexScan(keys, filterContext) overload.
                // If you need it later, add it in modb or filter on client side.
                return scan.runAsEmbedded(runtime.tx(), plan.keys);
            }

        } else {
            FullScan scan = new FullScan(index, plan.projects, entrySize);

            return (boundFilter == null)
                    ? scan.runAsEmbedded(runtime.tx())
                    : scan.runAsEmbedded(runtime.tx(), boundFilter);
        }
    }

    /**
     * Replace ParamRef(?i) placeholders inside FilterContext with actual values.
     */
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