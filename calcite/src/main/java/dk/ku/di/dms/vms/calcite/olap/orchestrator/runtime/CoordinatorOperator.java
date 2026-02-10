package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import java.util.List;


public interface CoordinatorOperator {
    void open();
    List<Object[]> nextBatch();
    void close();
}