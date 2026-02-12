package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.DistributedPlan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DistributedExecutorTest {

    @Mock VmsHttpClient httpClient;

    @Test
    void testExecuteSimpleScan() {
        // 1. Construct a Plan: Project -> Scan
        // Scan Def
        ScanDefinition scanDef = new ScanDefinition("ex_1", List.of("colA"));
        VmsSubplan subplan = new VmsSubplan("vms1", "url", "ex_1", new ScanAllOperation("s", "t"), List.of("colA"));

        // Project Def
        ProjectDefinition projectDef = new ProjectDefinition(scanDef, new int[]{0});

        DistributedPlan plan = new DistributedPlan(10L, List.of(subplan), projectDef);

        // 2. Mock HTTP
        List<Object> row = List.of("Success");
        PushdownResponse resp = new PushdownResponse("vms1", 10L, List.of("colA"), List.of(row));

        when(httpClient.executeScanAll(any(), any())).thenReturn(resp);

        // 3. Execute
        DistributedExecutor executor = new DistributedExecutor(httpClient);
        PushdownResponse result = executor.execute(plan);

        // 4. Assert
        assertNotNull(result);
        assertEquals(1, result.rows.size());
        assertEquals("Success", result.rows.get(0).get(0));
    }
}