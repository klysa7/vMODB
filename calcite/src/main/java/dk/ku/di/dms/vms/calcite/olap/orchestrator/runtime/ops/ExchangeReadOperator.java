package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.lang.System.Logger.Level.INFO;

/**
 * Reads from a VMS. This is the "Pull" from the network.
 */
public class ExchangeReadOperator implements CoordinatorOperator {

    private final VmsHttpClient http;
    private final VmsSubplan subplan;
    private final Long snapshot;

    // NEW: The Dynamic Filter (Semi-Join)
    private Set<Object> runtimeFilter;

    private Iterator<List<Object>> rowIterator;

    public ExchangeReadOperator(VmsHttpClient http, VmsSubplan subplan, Long snapshot) {
        this.http = http;
        this.subplan = subplan;
        this.snapshot = snapshot;
        this.runtimeFilter = null; // Default is empty
    }

    private static final System.Logger LOGGER = System.getLogger(ExchangeReadOperator.class.getName());

    // Called by the Join Operator in Phase 2
    public void setDynamicFilter(Set<Object> keys) {
        this.runtimeFilter = keys;
    }

    @Override
    public void open() {
        LOGGER.log(INFO,"I entered open read of the Exchange Read Operator");


        if (runtimeFilter != null && !runtimeFilter.isEmpty()) {

        }
        PushdownResponse response = http.executeScanAll(subplan, snapshot);

        if (response.rows != null) {
            this.rowIterator = response.rows.iterator();
        } else {
            this.rowIterator = Collections.emptyIterator();
        }
    }

    @Override
    public List<Object[]> nextBatch() {
        LOGGER.log(INFO,"I entered nextBatch read of the Exchange Read Operator");

        if (!rowIterator.hasNext()) return null;

        List<Object> row = rowIterator.next();

        return Collections.singletonList(row.toArray());
    }

    @Override
    public void close() {
        LOGGER.log(INFO,"I entered close of the Exchange Read Operator");
    }
}