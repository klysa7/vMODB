package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;

import java.util.ArrayList;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public class LocalProjectOperator implements CoordinatorOperator {

    private static final System.Logger LOGGER = System.getLogger(LocalProjectOperator.class.getName());


    private final CoordinatorOperator input;
    private final int[] projectedIndices;

    public LocalProjectOperator(CoordinatorOperator input, int[] projectedIndices) {
        this.input = input;
        this.projectedIndices = projectedIndices;
    }



    @Override
    public void open() {
        LOGGER.log(INFO,"I entered open read of theproject");

        input.open();
    }

    @Override
    public List<Object[]> nextBatch() {
//        LOGGER.log(INFO, "nextBatch() called! Asking input for data...");

        List<Object[]> inputBatch = input.nextBatch();

        if (inputBatch == null) {
//            LOGGER.log(INFO, "Input returned NULL! Pipeline is stopping.");
            return null;
        }

        List<Object[]> outputBatch = new ArrayList<>(inputBatch.size());

        for (Object[] inRow : inputBatch) {
            Object[] outRow = new Object[projectedIndices.length];
            for (int i = 0; i < projectedIndices.length; i++) {
                int sourceIndex = projectedIndices[i];
                if (sourceIndex < inRow.length) {
                    outRow[i] = inRow[sourceIndex];
                } else {
                    outRow[i] = null;
                }
            }
            outputBatch.add(outRow);
        }

//        LOGGER.log(INFO, "Returning projected batch of size: " + outputBatch.size());
        return outputBatch;
    }

    @Override
    public void close() {
        LOGGER.log(INFO,"I entered close project");

        input.close();
    }
}