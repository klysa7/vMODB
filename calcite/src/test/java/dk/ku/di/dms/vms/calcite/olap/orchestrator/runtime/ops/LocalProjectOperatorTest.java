package dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.ops;

import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.CoordinatorOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LocalProjectOperatorTest {

    @Mock CoordinatorOperator inputOp;

    @Test
    void testProjectionReordering() {
        // Input: [100, "Device", 99.99]
        Object[] inputRow = new Object[]{100, "Device", 99.99};
        when(inputOp.nextBatch()).thenReturn(Collections.singletonList(inputRow), null);

        // Project: Select [Name (1), Price (2)] -> Drop ID (0)
        int[] projection = new int[]{1, 2};
        LocalProjectOperator project = new LocalProjectOperator(inputOp, projection);

        project.open();
        List<Object[]> result = project.nextBatch();

        assertNotNull(result);
        Object[] row = result.get(0);

        assertEquals(2, row.length);
        assertEquals("Device", row[0]); // Was index 1
        assertEquals(99.99, row[1]);  // Was index 2
    }
}