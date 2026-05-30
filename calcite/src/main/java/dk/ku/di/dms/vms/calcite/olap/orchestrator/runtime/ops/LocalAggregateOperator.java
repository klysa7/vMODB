package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.AggregateDefinition;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;

import java.util.*;

import static java.lang.System.Logger.Level.INFO;

/**
 * Local hash aggregation at the gateway. Accumulates COUNT/SUM/AVG/MIN/MAX
 * per group key into a double[] (AVG carries an extra count slot), then emits
 * one output row per group. Materialises all groups in open(); nextBatch()
 * pages the result in batches of {@code BATCH_SIZE}.
 */
public final class LocalAggregateOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(LocalAggregateOperator.class.getName());

    private final CoordinatorOperator input;
    private final int[] groupByIndices;
    private final List<AggregateDefinition.AggCallDef> aggCalls;
    private List<Object[]> results;
    private int resultIndex = 0;
    private static final int BATCH_SIZE = 1000;

    public LocalAggregateOperator(CoordinatorOperator input,
                                  int[] groupByIndices,
                                  List<AggregateDefinition.AggCallDef> aggCalls) {
        this.input = input;
        this.groupByIndices = groupByIndices;
        this.aggCalls = aggCalls;
    }

    @Override
    public void open() {
        input.open();


        int nAggs = aggCalls.size();
        int[] avgCountSlot = new int[nAggs];
        int totalSlots = nAggs;
        for (int i = 0; i < nAggs; i++) {
            if ("AVG".equals(aggCalls.get(i).kind())) {
                avgCountSlot[i] = totalSlots++;
            } else {
                avgCountSlot[i] = -1;
            }
        }
        final int slots = totalSlots;

        Map<GroupKey, double[]> accMap = new LinkedHashMap<>(4096);

        long totalInput = 0;
        List<Object[]> batch;
        while ((batch = input.nextBatch()) != null) {
            for (Object[] row : batch) {
                totalInput++;
                GroupKey key = extractGroupKey(row, groupByIndices);
                double[] acc = accMap.computeIfAbsent(key, _ -> initAccumulators(nAggs, slots, aggCalls));

                for (int i = 0; i < nAggs; i++) {
                    AggregateDefinition.AggCallDef call = aggCalls.get(i);
                    switch (call.kind()) {
                        case "COUNT" -> acc[i] += 1.0;
                        case "SUM"   -> {
                            double v = numericValue(row, call.argIndex());
                            if (!Double.isNaN(v)) acc[i] += v;
                        }
                        case "AVG"   -> {
                            double v = numericValue(row, call.argIndex());
                            if (!Double.isNaN(v)) {
                                acc[i] += v;
                                acc[avgCountSlot[i]] += 1.0;
                            }
                        }
                        case "MIN"   -> {
                            double v = numericValue(row, call.argIndex());
                            if (!Double.isNaN(v)) acc[i] = Math.min(acc[i], v);
                        }
                        case "MAX"   -> {
                            double v = numericValue(row, call.argIndex());
                            if (!Double.isNaN(v)) acc[i] = Math.max(acc[i], v);
                        }
                    }
                }
            }
        }

//        LOGGER.log(INFO, "Input rows: " + totalInput
//                + " | Groups: " + accMap.size());

        results = new ArrayList<>(accMap.size());
        for (Map.Entry<GroupKey, double[]> entry : accMap.entrySet()) {
            Object[] groupKeys = entry.getKey().keys;
            double[] acc      = entry.getValue();
            Object[] outRow   = new Object[groupKeys.length + nAggs];

            System.arraycopy(groupKeys, 0, outRow, 0, groupKeys.length);

            for (int i = 0; i < nAggs; i++) {
                AggregateDefinition.AggCallDef call = aggCalls.get(i);
                outRow[groupKeys.length + i] = switch (call.kind()) {
                    case "COUNT"           -> (long) acc[i];
                    case "AVG"             -> (acc[avgCountSlot[i]] > 0)
                            ? acc[i] / acc[avgCountSlot[i]]
                            : 0.0;
                    default -> acc[i];
                };
            }
            results.add(outRow);
        }
    }

    @Override
    public List<Object[]> nextBatch() {
        if (results == null || resultIndex >= results.size()) return null;

        int end = Math.min(resultIndex + BATCH_SIZE, results.size());
        List<Object[]> batch = new ArrayList<>(results.subList(resultIndex, end));
        resultIndex = end;
        return batch;
    }

    @Override
    public void close() {
        input.close();
        if (results != null) results.clear();
    }

    private static GroupKey extractGroupKey(Object[] row, int[] indices) {
        if (indices.length == 0) return GroupKey.GLOBAL;
        Object[] keys = new Object[indices.length];
        for (int i = 0; i < indices.length; i++) {
            keys[i] = (indices[i] < row.length) ? row[indices[i]] : null;
        }
        return new GroupKey(keys);
    }

    private static double[] initAccumulators(int nAggs, int slots,
                                             List<AggregateDefinition.AggCallDef> calls) {
        double[] acc = new double[slots];
        for (int i = 0; i < nAggs; i++) {
            switch (calls.get(i).kind()) {
                case "MIN" -> acc[i] = Double.MAX_VALUE;
                case "MAX" -> acc[i] = -Double.MAX_VALUE;
            }
        }
        return acc;
    }

    private static double numericValue(Object[] row, int argIndex) {
        if (argIndex < 0 || argIndex >= row.length) return Double.NaN;
        Object v = row[argIndex];
        if (v == null) return Double.NaN;
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(v.toString()); } catch (Exception e) { return Double.NaN; }
    }

    private static final class GroupKey {
        static final GroupKey GLOBAL = new GroupKey(new Object[0]);

        final Object[] keys;

        GroupKey(Object[] keys) { this.keys = keys; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof GroupKey g)) return false;
            return Arrays.equals(keys, g.keys);
        }

        @Override
        public int hashCode() { return Arrays.hashCode(keys); }
    }
}