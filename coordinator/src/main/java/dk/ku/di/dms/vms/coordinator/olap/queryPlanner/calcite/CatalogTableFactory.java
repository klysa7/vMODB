package dk.ku.di.dms.vms.coordinator.olap.queryPlanner.calcite;

import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CatalogTable;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CatalogType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

final class CatalogTableFactory {

    private CatalogTableFactory() {}

    static Map<String, Table> toCalciteTables(CoordinatorCatalog catalog, String schemaName) {
        Map<String, Table> tables = new HashMap<>();
        for (Map.Entry<String, CatalogTable> e : catalog.tablesInSchema(schemaName).entrySet()) {
            tables.put(e.getKey(), new CatalogBackedCalciteTable(e.getValue()));
        }
        return tables;
    }

    static final class CatalogBackedCalciteTable extends AbstractTable {
        private final CatalogTable table;

        CatalogBackedCalciteTable(CatalogTable table) {
            this.table = table;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(typeFactory);

            for (CatalogColumn c : table.columns()) {
                RelDataType t = mapType(typeFactory, c.type());
                if (c.nullable()) {
                    t = typeFactory.createTypeWithNullability(t, true);
                }
                b.add(c.name(), t);
            }
            return b.build();
        }

        private RelDataType mapType(RelDataTypeFactory tf, CatalogType t) {
            return switch (t) {
                case INT -> tf.createJavaType(Integer.class);
                case BIGINT -> tf.createJavaType(Long.class);
                case FLOAT -> tf.createJavaType(Float.class);
                case DOUBLE -> tf.createJavaType(Double.class);
                case BOOLEAN -> tf.createJavaType(Boolean.class);
                case VARCHAR -> tf.createJavaType(String.class);
                case DATE -> tf.createJavaType(Date.class);
                case TIMESTAMP -> tf.createJavaType(Timestamp.class);
                case BYTES -> tf.createJavaType(byte[].class);
                case LONG -> null;
                case BOOL -> null;
                case STRING -> null;
            };
        }
    }
}
