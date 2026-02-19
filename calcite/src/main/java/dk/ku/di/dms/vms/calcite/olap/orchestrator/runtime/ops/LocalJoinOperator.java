package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import java.util.*;

public class LocalJoinOperator implements CoordinatorOperator {

    private final CoordinatorOperator left;
    private final CoordinatorOperator right;
    private final int[] leftKeyIndices;
    private final int[] rightKeyIndices;
    private Map<List<Object>, List<Object[]>> buildTable;
    private List<Object[]> probeBatch;
    private int probeIndex = 0;
    private final List<Object[]> outputBuffer = new ArrayList<>();

    public LocalJoinOperator(CoordinatorOperator left, CoordinatorOperator right, int[] leftKeyIndices, int[] rightKeyIndices) {
        this.left = left;
        this.right = right;
        this.leftKeyIndices = leftKeyIndices;
        this.rightKeyIndices = rightKeyIndices;
    }

    @Override
    public void open() {
        left.open();
        right.open();

        this.buildTable = new HashMap<>();
        List<Object[]> batch;
        while ((batch = left.nextBatch()) != null) {
            for (Object[] row : batch) {
                List<Object> key = extractKey(row, leftKeyIndices);
                buildTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
    }

    @Override
    public List<Object[]> nextBatch() {
        while (outputBuffer.size() < 1000) {

            if (probeBatch == null || probeIndex >= probeBatch.size()) {
                probeBatch = right.nextBatch();
                probeIndex = 0;

                if (probeBatch == null) {
                    if (outputBuffer.isEmpty()) return null; // Truly done
                    break; // Return whatever partial buffer we have
                }
            }

            Object[] rightRow = probeBatch.get(probeIndex++);
            List<Object> key = extractKey(rightRow, rightKeyIndices);

            List<Object[]> matches = buildTable.get(key);
            if (matches != null) {
                for (Object[] leftRow : matches) {
                    outputBuffer.add(concat(leftRow, rightRow));
                }
            }
        }

        if (outputBuffer.isEmpty()) return null;

        List<Object[]> result = new ArrayList<>(outputBuffer);
        outputBuffer.clear();
        return result;
    }

    private List<Object> extractKey(Object[] row, int[] indices) {
        List<Object> key = new ArrayList<>(indices.length);
        for (int idx : indices) {
            key.add(row[idx]);
        }
        return key;
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
    }
}