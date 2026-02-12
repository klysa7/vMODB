package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.ScanAllOperation;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc.VmsHttpClient;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AsyncExchangeReadOperatorTest {

    @Mock
    private VmsHttpClient httpClient;

    private AsyncExchangeReadOperator operator;

    @AfterEach
    void tearDown() {
        if (operator != null) operator.close();
    }

    @Test
    void testOpenStartsThreadAndFetchesData() {
        // 1. SETUP: Create Dummy Subplan and Response
        VmsSubplan subplan = new VmsSubplan("vms1", "http://vms1", "ex_1", new ScanAllOperation("s", "t"), List.of("col1"));

        // Mock the HTTP Response (2 rows)
        List<Object> row1 = List.of(1, "Alice");
        List<Object> row2 = List.of(2, "Bob");
        PushdownResponse mockResponse = new PushdownResponse("vms1", 100L, List.of("id", "name"), List.of(row1, row2));

        when(httpClient.executeScanAll(eq(subplan), eq(100L))).thenReturn(mockResponse);

        operator = new AsyncExchangeReadOperator(httpClient, subplan, 100L);

        // 2. ACT: Open starts the background thread
        operator.open();

        // 3. ASSERT: Consume batches
        // Since it's async, nextBatch() might block slightly, but Mockito acts fast.

        // Batch 1
        List<Object[]> batch1 = operator.nextBatch();
        assertNotNull(batch1);
        assertEquals(1, batch1.size()); // 1 row per batch based on your implementation
        assertEquals("Alice", batch1.get(0)[1]);

        // Batch 2
        List<Object[]> batch2 = operator.nextBatch();
        assertNotNull(batch2);
        assertEquals("Bob", batch2.get(0)[1]);

        // End of Stream
        List<Object[]> batch3 = operator.nextBatch();
        assertNull(batch3, "Should return null after EOS");

        // Verify HTTP was called exactly once
        verify(httpClient, times(1)).executeScanAll(any(), any());
    }

    @Test
    void testEmptyResponse() {
        // Setup empty response
        VmsSubplan subplan = new VmsSubplan("vms1", "url", "ex_1", new ScanAllOperation("s", "t"), List.of());
        PushdownResponse emptyResponse = new PushdownResponse("vms1", 100L, List.of(), Collections.emptyList());

        when(httpClient.executeScanAll(any(), any())).thenReturn(emptyResponse);

        operator = new AsyncExchangeReadOperator(httpClient, subplan, 100L);
        operator.open();

        // Should return null immediately
        assertNull(operator.nextBatch());
    }
}