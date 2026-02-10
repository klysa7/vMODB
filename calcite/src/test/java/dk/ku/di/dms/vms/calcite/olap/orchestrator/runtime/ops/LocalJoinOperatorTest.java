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
class LocalJoinOperatorTest {

    @Mock CoordinatorOperator leftOp;
    @Mock CoordinatorOperator rightOp;

    @Test
    void testInnerJoinExecution() {
        // --- SETUP DATA ---
        // Left Side: [ID=1, Name="Alice"], [ID=2, Name="Bob"]
        List<Object[]> leftBatch1 = Collections.singletonList(new Object[]{1, "Alice"});
        List<Object[]> leftBatch2 = Collections.singletonList(new Object[]{2, "Bob"});

        // Right Side: [ID=1, Order="Ord_A"], [ID=3, Order="Ord_B"] (ID 3 shouldn't match)
        List<Object[]> rightBatch1 = Collections.singletonList(new Object[]{1, "Ord_A"});
        List<Object[]> rightBatch2 = Collections.singletonList(new Object[]{3, "Ord_B"});

        // Mock Sequence:
        // Left yields 2 batches then null
        when(leftOp.nextBatch()).thenReturn(leftBatch1, leftBatch2, null);
        // Right yields 2 batches then null
        when(rightOp.nextBatch()).thenReturn(rightBatch1, rightBatch2, null);

        // Join on Index 0 (ID)
        LocalJoinOperator join = new LocalJoinOperator(leftOp, rightOp, 0, 0);

        // --- ACT ---
        // 1. Open should trigger build phase
        join.open();

        // Verify Parallel Start: Both must be opened
        verify(leftOp).open();
        verify(rightOp).open();

        // Verify Build Phase: Left should be fully consumed inside open()
        verify(leftOp, times(3)).nextBatch();

        // 2. Consume Join Results
        List<Object[]> resultBatch = join.nextBatch();

        // --- ASSERT ---
        assertNotNull(resultBatch);
        // Should only match ID=1. (Alice + Ord_A)
        // [1, "Alice", 1, "Ord_A"]
        assertEquals(1, resultBatch.size());
        Object[] joinedRow = resultBatch.get(0);

        assertEquals(4, joinedRow.length);
        assertEquals("Alice", joinedRow[1]);
        assertEquals("Ord_A", joinedRow[3]);

        // Next call should be null (ID=3 didn't match, and stream ended)
        assertNull(join.nextBatch());
    }
}