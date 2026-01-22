package dk.ku.di.dms.vms.calcite.service;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogTable;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogColumnDto;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogTableDto;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CatalogAdapterTest {

    @Test
    void toCoordinatorCatalogSuccess() {
        CatalogColumnDto catalogColumnDto = mock(CatalogColumnDto.class);
        when(catalogColumnDto.name()).thenReturn("customer_id\n");
        when(catalogColumnDto.type()).thenReturn("INT");

        CatalogColumnDto catalogColumnDto1 = mock(CatalogColumnDto.class);
        when(catalogColumnDto1.name()).thenReturn("order_id");
        when(catalogColumnDto1.type()).thenReturn("BIGINT");

        CatalogTableDto catalogTableDto = mock(CatalogTableDto.class);
        when(catalogTableDto.columns()).thenReturn(List.of(catalogColumnDto, catalogColumnDto1));

        CatalogResponse catalogResponse = mock(CatalogResponse.class);
        when(catalogResponse.snapshotId()).thenReturn(7L);
        when(catalogResponse.schemas()).thenReturn(Map.of("order",
                Map.of("orders", catalogTableDto)));

        CoordinatorCatalog coordinatorCatalog = CatalogAdapter.toCoordinatorCatalog(catalogResponse);

        Map<String, CatalogTable> catalogTableMap = coordinatorCatalog.tablesInSchema("order");
        assertThat(catalogTableMap).containsKey("orders");

        CatalogTable orders = catalogTableMap.get("orders");

        assertThat(orders).isNotNull();
        assertThat(orders.columns()).hasSize(2);
        assertThat(orders.columns().get(0).name()).isEqualTo("customer_id");
        assertThat(orders.columns().get(1).name()).isEqualTo("order_id");
        assertThat(orders.columns().get(0).type().name()).isEqualTo("INT");
        assertThat(orders.columns().get(1).type().name()).isEqualTo("BIGINT");
        assertThat(orders.columns().get(0).nullable()).isTrue();
        assertThat(orders.columns().get(1).nullable()).isTrue();
    }

    @Test
    void toCoordinatorCatalogSuccessWithNullColumns() {
        CatalogTableDto catalogTableDto = mock(CatalogTableDto.class);
        when(catalogTableDto.columns()).thenReturn(null);

        CatalogResponse catalogResponse = mock(CatalogResponse.class);
        when(catalogResponse.snapshotId()).thenReturn(1L);
        when(catalogResponse.schemas()).thenReturn(Map.of("s",
                Map.of("t", catalogTableDto)));

        CoordinatorCatalog coordinatorCatalog = CatalogAdapter.toCoordinatorCatalog(catalogResponse);
        CatalogTable catalogTable = coordinatorCatalog.tablesInSchema("s").get("t");
        assertThat(catalogTable).isNotNull();
        assertThat(catalogTable.columns()).isEmpty();
    }
}