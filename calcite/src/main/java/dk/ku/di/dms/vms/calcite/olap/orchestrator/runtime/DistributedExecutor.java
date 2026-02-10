package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.Logger.Level.INFO;

public final class DistributedExecutor {

    private static final System.Logger LOGGER = System.getLogger(DistributedExecutor.class.getName());
    private final VmsHttpClient vmsHttpClient;

    public DistributedExecutor(VmsHttpClient vmsHttpClient) {
        this.vmsHttpClient = vmsHttpClient;
    }

    public PushdownResponse execute(DistributedPlan distributedPlan) {
        long start = System.currentTimeMillis();
        LOGGER.log(INFO, ">>> STH LIKE A TRINO DRIVER STARTING DRIVER LOOP FFOR SNAPSHOT #" + distributedPlan.snapshot);

        CoordinatorOperator root = buildOperatorTree(distributedPlan.root, distributedPlan);
        LOGGER.log(INFO, "--> [DRIVER] OPENING PIPELINE THE ROOT OPENS< ITS STARTING");
        root.open();

        List<List<Object>> allRows = new ArrayList<>();
        List<Object[]> batch;
        int batchCount = 0;

        LOGGER.log(INFO, "--> [DRIVER] ENTERING PROCESSING LOOP...");
        try {
            // This is the tight loop. In Trino, this checks isBlocked
            // but nextBatch() returns immediately if data is in the buffer
            while ((batch = root.nextBatch()) != null) {
                batchCount++;
                if (!batch.isEmpty()) {
                    System.out.println(" RESULT ROW: " + java.util.Arrays.toString(batch.get(0)));
                }
                for (Object[] row : batch) {
                    allRows.add(Arrays.asList(row));
                }
//                if (batchCount % 10 == 0) LOGGER.log(INFO, ">>> [DRIVER] Processed batch #" + batchCount);
            }
        } finally {
            LOGGER.log(INFO, ">>> [DRIVER] CLOSING PIPELINE");
            root.close();
        }

        LOGGER.log(INFO, "DRIVER] FINISHED. Total Rows: " + allRows.size() + " Time: " + (System.currentTimeMillis() - start) + "ms");
        return new PushdownResponse("gateway", distributedPlan.snapshot, null, allRows);
    }

    private CoordinatorOperator buildOperatorTree(CoordinatorOperatorDefinition coordinatorOperatorDefinition,
                                                  DistributedPlan distributedPlan) {

        if (coordinatorOperatorDefinition instanceof ScanDefinition scanDef) {
            VmsSubplan subplan = distributedPlan.subPlans.stream()
                    .filter(s -> s.exchangeId.equals(scanDef.exchangeId()))
                    .findFirst().orElseThrow();
            //do it asychronously
            return new AsyncExchangeReadOperator(vmsHttpClient, subplan, distributedPlan.snapshot);
        }
        if (coordinatorOperatorDefinition instanceof JoinDefinition joinDefinition) {
            return new LocalJoinOperator(
                    buildOperatorTree(joinDefinition.left(), distributedPlan),
                    buildOperatorTree(joinDefinition.right(), distributedPlan),
                    joinDefinition.leftKeyIndex(), joinDefinition.rightKeyIndex()
            );
        }
        if (coordinatorOperatorDefinition instanceof ProjectDefinition projectDefinition) {
            return new LocalProjectOperator(
                    buildOperatorTree(projectDefinition.input(), distributedPlan),
                    projectDefinition.projectedIndices()
            );
        }
        throw new IllegalArgumentException("Unknown Op: " + coordinatorOperatorDefinition);
    }
}