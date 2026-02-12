package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import java.util.*;
import static java.lang.System.Logger.Level.INFO;

public class LocalJoinOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(LocalJoinOperator.class.getName());

    private final CoordinatorOperator left;
    private final CoordinatorOperator right;
    private final Integer leftKeyIdx;
    private final Integer rightKeyIdx;
    private Map<Object, List<Object[]>> buildTable;
    private Iterator<Object[]> currentProbeBatch;

    public LocalJoinOperator(CoordinatorOperator left, CoordinatorOperator right, Integer leftKeyIdx, Integer rightKeyIdx) {
        this.left = left;
        this.right = right;
        this.leftKeyIdx = leftKeyIdx;
        this.rightKeyIdx = rightKeyIdx;
    }


    private Object toJoinKey(Object input) {
        if (input instanceof Number) {
            return ((Number) input).longValue();
        }
        return input;
    }

    @Override
    public void open() {
        LOGGER.log(INFO, "[LocalJoin] OPENING BOTH SIDES (Parallel Gather)...");
        left.open();
        right.open();

        this.buildTable = new HashMap<>();
        List<Object[]> batch;
        long buildCount = 0;

        //build teh left side
        while ((batch = left.nextBatch()) != null) {
            for (Object[] row : batch) {
                Object rawKey = row[leftKeyIdx];
                Object key = toJoinKey(rawKey);

                if (key != null) {
                    buildTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                }
                buildCount++;
            }
        }

        String keyType = buildTable.isEmpty() ? "None" : buildTable.keySet().iterator().next().getClass().getSimpleName();
        LOGGER.log(INFO, "[LocalJoin] Build Complete. Total Rows: " + buildCount + " | Unique Keys: " + buildTable.size() + " | Key Type: " + keyType);
    }

    @Override
    public List<Object[]> nextBatch() {
        // the probe - right side
        List<Object[]> outputBatch = new ArrayList<>();

        while (outputBatch.size() < 100) {
            if (currentProbeBatch == null || !currentProbeBatch.hasNext()) {
                List<Object[]> rightBatch = right.nextBatch();
                if (rightBatch == null) break;
                currentProbeBatch = rightBatch.iterator();
            }

            Object[] rightRow = currentProbeBatch.next();

            Object rawRightKey = rightRow[rightKeyIdx];
            Object rightKey = toJoinKey(rawRightKey);

            List<Object[]> leftMatches = buildTable.get(rightKey);
            if (leftMatches != null) {
                for (Object[] leftRow : leftMatches) {
                    outputBatch.add(merge(leftRow, rightRow));
                }
            }
        }
        return outputBatch.isEmpty() ? null : outputBatch;
    }

    private Object[] merge(Object[] left, Object[] right) {
        Object[] joined = new Object[left.length + right.length];
        System.arraycopy(left, 0, joined, 0, left.length);
        System.arraycopy(right, 0, joined, left.length, right.length);
        return joined;
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }
}