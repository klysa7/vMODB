package dk.ku.di.dms.vms.calcite.queryPlanner;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.calcite.CalciteSchemaBuilder;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.calcite.service.CatalogAdapter;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogColumnDto;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogTableDto;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class CalciteSchemaBuilderTest {

    @Test
    void buildRootSchema() {
        CatalogColumnDto catalogColumnDto = mock(CatalogColumnDto.class);
        when(catalogColumnDto.name()).thenReturn("customer_id");
        when(catalogColumnDto.type()).thenReturn("INT");

        CatalogColumnDto catalogColumnDto1 = mock(CatalogColumnDto.class);
        when(catalogColumnDto1.name()).thenReturn("order_id");
        when(catalogColumnDto1.type()).thenReturn("BIGINT");

        CatalogTableDto catalogTableDto = mock(CatalogTableDto.class);
        when(catalogTableDto.columns()).thenReturn(List.of(catalogColumnDto, catalogColumnDto1));

        CatalogResponse catalogResponse = mock(CatalogResponse.class);
        when(catalogResponse.snapshotId()).thenReturn(1L);
        when(catalogResponse.schemas()).thenReturn(Map.of("order",
                Map.of("orders", catalogTableDto)));

        CoordinatorCatalog coordinatorCatalog = CatalogAdapter.toCoordinatorCatalog(catalogResponse);

        SchemaPlus root = new CalciteSchemaBuilder().buildRootSchema(coordinatorCatalog);

        SchemaPlus orderSchema = root.getSubSchema("order");
        assertThat(orderSchema).isNotNull();

        Table calciteTable = orderSchema.getTable("orders");
        assertThat(calciteTable).isNotNull();
    }
}