package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import java.util.*;

public final class CoordinatorHashJoin {

    private static final System.Logger LOGGER =
            System.getLogger(CoordinatorHashJoin.class.getName());

    public PushdownResponse join(PushdownResponse left, PushdownResponse right,
                                 int leftKeyIndex, int rightKeyIndex) {

        if (leftKeyIndex < 0 || leftKeyIndex >= left.width())
            throw new IllegalArgumentException("left out of range: " + leftKeyIndex);
        if (rightKeyIndex < 0 || rightKeyIndex >= right.width())
            throw new IllegalArgumentException("right out of range: " + rightKeyIndex);

        boolean buildLeft = left.size() <= right.size();
        PushdownResponse build = buildLeft ? left : right;
        PushdownResponse probe = buildLeft ? right : left;

        int buildKey = buildLeft ? leftKeyIndex : rightKeyIndex;
        int probeKey = buildLeft ? rightKeyIndex : leftKeyIndex;

        Map<Object, List<List<Object>>> ht = new HashMap<>();
        for (var row : build.rows) {
            Object key = row.get(buildKey);
            ht.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }

        List<String> outCols = new ArrayList<>(left.columns.size() + right.columns.size());
        outCols.addAll(left.columns);
        outCols.addAll(right.columns);

        List<List<Object>> outRows = new ArrayList<>();

        probe.rows.forEach(prow -> {
            Object key = prow.get(probeKey);
            var matches = ht.get(key);
            if (matches == null) return; // same as continue

            matches.forEach(brow -> {
                List<Object> joined = new ArrayList<>(left.width() + right.width());
                if (buildLeft) {
                    joined.addAll(brow);
                    joined.addAll(prow);
                } else {
                    joined.addAll(prow);
                    joined.addAll(brow);
                }
                outRows.add(joined);
            });
        });

        return new PushdownResponse("coordinator", left.snapshot, outCols, outRows);
    }
}