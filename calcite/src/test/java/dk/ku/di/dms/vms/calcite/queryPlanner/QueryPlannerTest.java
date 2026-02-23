package dk.ku.di.dms.vms.calcite.queryPlanner;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.QueryPlanner;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlanner;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class QueryPlannerTest {

    @Test
    void buildRootSchema() {
        CoordinatorCatalog coordinatorCatalog = mock(CoordinatorCatalog.class);
        CalciteSchemaBuilder calciteSchemaBuilder = mock(CalciteSchemaBuilder.class);
        CalcitePlanner calcitePlanner = mock(CalcitePlanner.class);
        SchemaPlus rootSchema = mock(SchemaPlus.class);

        when(calciteSchemaBuilder.buildRootSchema(coordinatorCatalog)).thenReturn(rootSchema);

        CalcitePlanner.PlanOutput planOutput = mock(CalcitePlanner.PlanOutput.class);
        when(calcitePlanner.planJoinOnly(eq("SELECT 1"), same(rootSchema), eq(List.of(1, 2))))
                .thenReturn(planOutput);

        QueryPlanner qp = new QueryPlanner(coordinatorCatalog, calciteSchemaBuilder, calcitePlanner);
        CalcitePlanner.PlanOutput out = qp.plan("SELECT 1", List.of(1, 2));

        assertThat(out).isSameAs(planOutput);
        verify(calciteSchemaBuilder).buildRootSchema(coordinatorCatalog);
        verify(calcitePlanner).planJoinOnly(eq("SELECT 1"), same(rootSchema), eq(List.of(1, 2)));
        verifyNoMoreInteractions(calciteSchemaBuilder, calcitePlanner);
    }
}
