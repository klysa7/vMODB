package dk.ku.di.dms.vms.calcite.service;

import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogColumn;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogTable;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CatalogType;
import dk.ku.di.dms.vms.calcite.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogColumnDto;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogTableDto;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stateless converter that maps the coordinator's wire-format
 * {@link CatalogResponse} into the in-memory {@link CoordinatorCatalog}
 * consumed by the Calcite planner. Walks every schema/table/column entry,
 * normalises column names, maps string type tags to {@link CatalogType},
 * and constructs {@link CatalogTable} reflectively to tolerate either a
 * two-arg or three-arg constructor. Propagates the snapshot id from the
 * response into the resulting catalog.
 */
public final class CatalogAdapter {

    private CatalogAdapter() {}

    public static CoordinatorCatalog toCoordinatorCatalog(CatalogResponse catalogResponse) {
        CoordinatorCatalog coordinatorCatalog = new CoordinatorCatalog();
        coordinatorCatalog.setSnapshotId(catalogResponse.snapshotId());

        for (Map.Entry<String, Map<String, CatalogTableDto>> schemaEntry : catalogResponse.schemas().entrySet()) {
            String schema = schemaEntry.getKey();
            Map<String, CatalogTableDto> tables = schemaEntry.getValue();

            for (Map.Entry<String, CatalogTableDto> tableEntry : tables.entrySet()) {
                String tableName = tableEntry.getKey();
                CatalogTableDto catalogTableDto = tableEntry.getValue();

                List<CatalogColumn> cols = new ArrayList<>();
                if (catalogTableDto.columns() != null) {
                    for (CatalogColumnDto c : catalogTableDto.columns()) {
                        cols.add(newCatalogColumn(c.name(), c.type(), c.byteSize()));
                    }
                }

                CatalogTable table = newCatalogTable(tableName, cols);
                coordinatorCatalog.addTable(schema, table, catalogTableDto.ownerVms());
            }
        }
        return coordinatorCatalog;
    }

    private static CatalogTable newCatalogTable(String tableName, List<CatalogColumn> columns) {
        try {
            Constructor<CatalogTable> ctor = CatalogTable.class.getConstructor(String.class, List.class);
            return ctor.newInstance(tableName, columns);
        } catch (NoSuchMethodException ignored) {
            for (Constructor<?> ctor : CatalogTable.class.getConstructors()) {
                Class<?>[] p = ctor.getParameterTypes();
                try {
                    if (p.length == 3 && p[0] == String.class && List.class.isAssignableFrom(p[1])) {
                        Object third = defaultValueFor(p[2]);
                        return (CatalogTable) ctor.newInstance(tableName, columns, third);
                    }
                } catch (Exception ignored2) {}
            }
            throw new RuntimeException("No usable CatalogTable constructor found " + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct CatalogTable(" + tableName + "): " + e.getMessage(), e);
        }
    }

    private static CatalogColumn newCatalogColumn(String name, String type, int dtoByteSize) {
        String cleanName = sanitize(name);
        CatalogType ct = mapCatalogType(type);
        return new CatalogColumn(cleanName, ct, true, dtoByteSize);
    }

    private static String sanitize(String value) {
        if (value == null) return null;
        return value.replace("\r", "").replace("\n", "").trim();
    }

    private static Object defaultValueFor(Class<?> t) {
        if (t == boolean.class || t == Boolean.class) return true;
        if (t == int.class    || t == Integer.class)  return 0;
        if (t == long.class   || t == Long.class)     return 0L;
        if (t == String.class) return "";
        return null;
    }

    private static CatalogType mapCatalogType(String t) {
        if (t == null) return CatalogType.VARCHAR;
        return switch (sanitize(t).toUpperCase()) {
            case "INT", "INTEGER"          -> CatalogType.INT;
            case "BIGINT", "LONG"          -> CatalogType.BIGINT;
            case "FLOAT"                   -> CatalogType.FLOAT;
            case "DOUBLE"                  -> CatalogType.DOUBLE;
            case "BOOLEAN", "BOOL"         -> CatalogType.BOOLEAN;
            case "DATE"                    -> CatalogType.DATE;
            case "TIMESTAMP"               -> CatalogType.TIMESTAMP;
            case "VARCHAR", "STRING", "TEXT", "CHAR" -> CatalogType.VARCHAR;
            default                        -> CatalogType.VARCHAR;
        };
    }
}