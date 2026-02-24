package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import java.util.*;

public class LocalJoinOperator implements CoordinatorOperator {

    private final CoordinatorOperator left;
    private final CoordinatorOperator right;
    private final int[] leftKeyIndices;
    private final int[] rightKeyIndices;

    private Map<JoinKey, List<Object[]>> buildTable;
    private List<Object[]> probeBatch;
    private int probeIndex = 0;
    private final List<Object[]> outputBuffer = new ArrayList<>();

    private long buildTimeMs = 0;
    private long probeStartTime = 0;
    private long totalLeftRows = 0;
    private long totalMatches = 0;
    private long totalRightRowsProbed = 0;
    private long ghostRecordsDropped = 0;

    public LocalJoinOperator(CoordinatorOperator left, CoordinatorOperator right, int[] leftKeyIndices, int[] rightKeyIndices) {
        this.left = left;
        this.right = right;
        this.leftKeyIndices = leftKeyIndices;
        this.rightKeyIndices = rightKeyIndices;
    }

    @Override
    public void open() {
        System.out.println("\n==========================================");
        System.out.println("[BENCHMARK-JOIN] Phase 1: BUILD STARTING...");
        long start = System.currentTimeMillis();

        left.open();
        right.open();

        this.buildTable = new HashMap<>(100_000);
        List<Object[]> batch;

        while ((batch = left.nextBatch()) != null) {
            for (Object[] row : batch) {
                JoinKey key = extractKey(row, leftKeyIndices);
                if (key != null) {
                    buildTable.computeIfAbsent(key, k -> new ArrayList<>(1)).add(row);
                    totalLeftRows++;
                } else {
                    ghostRecordsDropped++;
                }
            }
            if ((totalLeftRows + ghostRecordsDropped) % 10000 == 0) {
                System.out.println("[BENCHMARK-JOIN] Buffered " + totalLeftRows + " valid rows... (Dropped " + ghostRecordsDropped + " ghosts)");
            }
        }

        buildTimeMs = System.currentTimeMillis() - start;
        System.out.println("[BENCHMARK-JOIN] Phase 1: BUILD COMPLETED!");
        System.out.println("[BENCHMARK-JOIN] Valid Left Rows Buffered: " + totalLeftRows);
        System.out.println("[BENCHMARK-JOIN] Build Time: " + buildTimeMs + " ms");
        System.out.println("==========================================\n");

        System.out.println("[BENCHMARK-JOIN] Phase 2: PROBE STARTING...");
        probeStartTime = System.currentTimeMillis();
    }

    @Override
    public List<Object[]> nextBatch() {
        while (outputBuffer.size() < 1000) {

            if (probeBatch == null || probeIndex >= probeBatch.size()) {
                probeBatch = right.nextBatch();
                probeIndex = 0;

                if (probeBatch == null) {
                    if (outputBuffer.isEmpty()) {
                        long probeTimeMs = System.currentTimeMillis() - probeStartTime;
                        System.out.println("\n==========================================");
                        System.out.println("[BENCHMARK-JOIN] Phase 2: PROBE COMPLETED!");
                        System.out.println("[BENCHMARK-JOIN] Total Right Rows Scanned: " + totalRightRowsProbed);
                        System.out.println("[BENCHMARK-JOIN] Total Matches Found: " + totalMatches);
                        System.out.println("[BENCHMARK-JOIN] Probe Time: " + probeTimeMs + " ms");
                        System.out.println("[BENCHMARK-JOIN] TOTAL JOIN TIME: " + (buildTimeMs + probeTimeMs) + " ms");
                        System.out.println("==========================================\n");
                        return null;
                    }
                    break;
                }
            }

            Object[] rightRow = probeBatch.get(probeIndex++);
            totalRightRowsProbed++;

            JoinKey key = extractKey(rightRow, rightKeyIndices);
            if (key != null) {
                List<Object[]> matches = buildTable.get(key);
                if (matches != null) {
                    for (Object[] leftRow : matches) {
                        outputBuffer.add(concat(leftRow, rightRow));
                        totalMatches++;
                    }
                }
            }

            if (totalRightRowsProbed % 10000 == 0) {
                System.out.println("[BENCHMARK-JOIN] Probed " + totalRightRowsProbed + " rows, found " + totalMatches + " matches...");
            }
        }

        if (outputBuffer.isEmpty()) return null;

        List<Object[]> result = new ArrayList<>(outputBuffer);
        outputBuffer.clear();
        return result;
    }

    private JoinKey extractKey(Object[] row, int[] indices) {
        Object[] keyData = new Object[indices.length];
        boolean allZero = true;

        for (int i = 0; i < indices.length; i++) {
            Object val = row[indices[i]];
            keyData[i] = val;

            if (val != null) {
                if (val instanceof Integer && ((Integer) val) != 0) allZero = false;
                else if (val instanceof Long && ((Long) val) != 0L) allZero = false;
                else if (val instanceof String && !((String) val).trim().isEmpty()) allZero = false;
            }
        }

        if (allZero) {
            return null;
        }

        return new JoinKey(keyData);
    }

    private Object[] concat(Object[] left, Object[] right) {
        Object[] result = new Object[left.length + right.length];
        System.arraycopy(left, 0, result, 0, left.length);
        System.arraycopy(right, 0, result, left.length, right.length);
        return result;
    }

    @Override
    public void close() {
        left.close();
        right.close();
        buildTable.clear();
    }

    private static final class JoinKey {
        private final Object[] keys;

        public JoinKey(Object[] keys) {
            this.keys = keys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinKey joinKey = (JoinKey) o;
            return Arrays.equals(keys, joinKey.keys);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }
    }
}