package dk.ku.di.dms.vms.calcite.queryPlanner.planner;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlanner;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.planner.CalcitePlannerImpl;
import dk.ku.di.dms.vms.calcite.service.CatalogAdapter;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogColumnDto;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogTableDto;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CalcitePlannerImplTest {

    @Test
    void plainJoinOnlyNotFull() {
        CatalogColumnDto catalogColumnDto = mock(CatalogColumnDto.class);
        when(catalogColumnDto.name()).thenReturn("customer_id");
        when(catalogColumnDto.type()).thenReturn("INT");

        CatalogColumnDto catalogColumnDto1 = mock(CatalogColumnDto.class);
        when(catalogColumnDto1.name()).thenReturn("order_id");
        when(catalogColumnDto1.type()).thenReturn("BIGINT");

        CatalogTableDto tableDto = mock(CatalogTableDto.class);
        when(tableDto.columns()).thenReturn(List.of(catalogColumnDto, catalogColumnDto1));

        CatalogResponse resp = mock(CatalogResponse.class);
        when(resp.snapshotId()).thenReturn(1L);
        when(resp.schemas()).thenReturn(Map.of("order",
                Map.of("orders", tableDto)));

        CoordinatorCatalog coordinatorCatalog = CatalogAdapter.toCoordinatorCatalog(resp);
        SchemaPlus root = new CalciteSchemaBuilder().buildRootSchema(coordinatorCatalog);

        CalcitePlannerImpl planner = new CalcitePlannerImpl();

        String sql = """
            SELECT o.customer_id
            FROM "order".orders o
            """;

        CalcitePlanner.PlanOutput out = planner.planJoinOnly(sql, root, List.of());

        assertThat(out).isNotNull();
        assertThat(out.logicalPlan()).isNotNull();
        assertThat(out.vmodbPhysicalPlan()).isNotNull();
    }
}