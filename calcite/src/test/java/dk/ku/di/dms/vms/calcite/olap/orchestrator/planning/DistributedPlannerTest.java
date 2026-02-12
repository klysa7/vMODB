package dk.ku.di.dms.vms.calcite.olap.orchestrator.planning;

import dk.ku.di.dms.vms.calcite.modb.rel.VModbTableAccess;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.placement.PlacementResolver;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.ops.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedPlannerTest {

    @Mock
    private PlacementResolver placement;

    @Mock
    private DistributedPlanner.ColumnsResolver columnsResolver;

    @InjectMocks
    private DistributedPlanner planner;


    @ParameterizedTest(name = "{0}")
    @MethodSource("provideJoinScenarios")
    void testPlanTransformation(String description, RelNode inputPlan, boolean shouldSucceed, String expectedMessagePart) {

        if (shouldSucceed) {
            DistributedPlan result = planner.create(inputPlan, 100L);

            assertNotNull(result);
            assertEquals(100L, result.snapshot);
            assertNotNull(result.root);
            assertFalse(result.subPlans.isEmpty(), "Should have created VMS subplans");

            if (inputPlan instanceof Project) {
                assertInstanceOf(ProjectDefinition.class, result.root);
            } else if (inputPlan instanceof Join) {
                assertInstanceOf(JoinDefinition.class, result.root);
            }

        } else {
            Exception exception = assertThrows(IllegalArgumentException.class, () -> {
                planner.create(inputPlan, 100L);
            });

            assertTrue(exception.getMessage().contains(expectedMessagePart),
                    "Error message should contain: " + expectedMessagePart + " but was: " + exception.getMessage());
        }
    }


    private static Stream<Arguments> provideJoinScenarios() {
        return Stream.of(
                Arguments.of("Standard Equi-Join: Order JOIN Payment",
                mockProject(mockJoin(mockScan("order", "orders", 5),
                mockScan("payment", "payments", 5),0, 5 + 0)), true, "N/A"),

                Arguments.of("Nested 3-Way Join: (A JOIN B) JOIN C",
                mockJoin(mockJoin(mockScan("vms1", "tableA", 2),
                mockScan("vms2", "tableB", 2),0, 2),
                mockScan("vms3", "tableC", 2),0, 4), true, "N/A")
//                ,
//
//                Arguments.of("Unsupported: Non-Equi Join (GREATER_THAN)",
//                mockJoinWithKind(mockScan("A", "tab", 2),
//                mockScan("B", "tab", 2), SqlKind.GREATER_THAN), false,
//                        "only supports equal join condition"),
//
//                Arguments.of("Unsupported: Sort Operator",
//                mockSort(mockScan("A", "tab", 1)),
//                        false, "Unsupported Operator")
        );
    }

    private static VModbTableAccess mockScan(String schema, String table, int fieldCount) {
        VModbTableAccess scan = mock(VModbTableAccess.class);
        when(scan.getSchemaName()).thenReturn(schema);
        when(scan.getTableName()).thenReturn(table);
        when(scan.getInputs()).thenReturn(Collections.emptyList());

        RelDataType rowType = mock(RelDataType.class);
        lenient().when(rowType.getFieldCount()).thenReturn(fieldCount);
        lenient().when(scan.getRowType()).thenReturn(rowType);

        return scan;
    }

    private static Project mockProject(RelNode input) {
        Project project = mock(Project.class);
        when(project.getInput()).thenReturn(input);

        RexInputRef ref = mock(RexInputRef.class);
        when(ref.getIndex()).thenReturn(0);
        when(project.getProjects()).thenReturn(List.of(ref));

        return project;
    }

    private static Join mockJoin(RelNode left, RelNode right, int leftIdx, int rightIdx) {
        Join join = mock(Join.class);
        when(join.getLeft()).thenReturn(left);
        when(join.getRight()).thenReturn(right);

        RelDataType leftType = mock(RelDataType.class);
        when(leftType.getFieldCount()).thenReturn(leftIdx < rightIdx ? rightIdx - leftIdx : 10); // Simple hack to ensure border logic works
        when(left.getRowType()).thenReturn(leftType);

        RexCall condition = mock(RexCall.class);
        when(condition.getKind()).thenReturn(SqlKind.EQUALS);

        RexInputRef op1 = mock(RexInputRef.class); when(op1.getIndex()).thenReturn(leftIdx);
        RexInputRef op2 = mock(RexInputRef.class); when(op2.getIndex()).thenReturn(rightIdx);

        when(condition.getOperands()).thenReturn(List.of(op1, op2));
        when(join.getCondition()).thenReturn(condition);

        return join;
    }

    private static Join mockJoinWithKind(RelNode left, RelNode right, SqlKind kind) {
        Join join = mock(Join.class);
        RexCall condition = mock(RexCall.class);
        when(condition.getKind()).thenReturn(kind);
        when(join.getCondition()).thenReturn(condition);
        return join;
    }

    private static Sort mockSort(RelNode input) {
        Sort sort = mock(Sort.class);

        when(sort.getInputs()).thenReturn(List.of(input));
        return sort;
    }
}