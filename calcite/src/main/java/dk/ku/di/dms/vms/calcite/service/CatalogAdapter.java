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

public final class CatalogAdapter {

    private CatalogAdapter() {}

    public static CoordinatorCatalog toCoordinatorCatalog(CatalogResponse dto) {
        CoordinatorCatalog cat = new CoordinatorCatalog();
        cat.setSnapshotId(dto.snapshotId());

        for (Map.Entry<String, Map<String, CatalogTableDto>> schemaEntry : dto.schemas().entrySet()) {
            String schema = schemaEntry.getKey();
            Map<String, CatalogTableDto> tables = schemaEntry.getValue();

            for (Map.Entry<String, CatalogTableDto> tableEntry : tables.entrySet()) {
                String tableName = tableEntry.getKey();
                CatalogTableDto t = tableEntry.getValue();

                List<CatalogColumn> cols = new ArrayList<>();
                if (t.columns() != null) {
                    for (CatalogColumnDto c : t.columns()) {
                        cols.add(newCatalogColumn(c.name(), c.type()));
                    }
                }

                CatalogTable table = newCatalogTable(tableName, cols);
                cat.addTable(schema, table, t.ownerVms());
            }
        }

        return cat;
    }

    // ---- construct CatalogTable (handle your constructor signature safely) ----
    private static CatalogTable newCatalogTable(String tableName, List<CatalogColumn> cols) {
        try {
            // Try (String, List<CatalogColumn>)
            Constructor<CatalogTable> ctor = CatalogTable.class.getConstructor(String.class, List.class);
            return ctor.newInstance(tableName, cols);
        } catch (NoSuchMethodException ignored) {
            // Try (String, List<CatalogColumn>, boolean) or (String, List, X)
            for (Constructor<?> ctor : CatalogTable.class.getConstructors()) {
                Class<?>[] p = ctor.getParameterTypes();
                try {
                    if (p.length == 3 && p[0] == String.class && List.class.isAssignableFrom(p[1])) {
                        Object third = defaultValueFor(p[2]);
                        return (CatalogTable) ctor.newInstance(tableName, cols, third);
                    }
                } catch (Exception ignored2) {}
            }
            throw new RuntimeException("No usable CatalogTable constructor found. tableName=" + tableName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to construct CatalogTable(" + tableName + "): " + e.getMessage(), e);
        }
    }

    private static CatalogColumn newCatalogColumn(String name, String type) {
        String cleanName = sanitize(name);
        CatalogType ct = mapCatalogType(type);
        // nullable: safest default for planning (you can refine later)
        return new CatalogColumn(cleanName, ct, true);
    }

    private static String sanitize(String s) {
        if (s == null) return null;
        return s.replace("\r", "").replace("\n", "").trim();
    }

    private static String mapType(String t) {
        if (t == null) return "VARCHAR";
        return switch (t.toUpperCase()) {
            case "INT", "INTEGER" -> "INTEGER";
            case "BIGINT", "LONG" -> "BIGINT";
            case "FLOAT" -> "FLOAT";
            case "DOUBLE" -> "DOUBLE";
            case "DATE" -> "DATE";
            case "TIMESTAMP" -> "TIMESTAMP";
            case "VARCHAR", "STRING", "TEXT" -> "VARCHAR";
            default -> "VARCHAR";
        };
    }

    private static Object defaultValueFor(Class<?> t) {
        if (t == boolean.class || t == Boolean.class) return true;
        if (t == int.class || t == Integer.class) return 0;
        if (t == long.class || t == Long.class) return 0L;
        if (t == String.class) return "";
        return null;
    }

    private static CatalogType mapCatalogType(String t) {
        if (t == null) return CatalogType.VARCHAR;

        return switch (sanitize(t).toUpperCase()) {
            case "INT", "INTEGER" -> CatalogType.INT;
            case "BIGINT", "LONG" -> CatalogType.BIGINT;
            case "FLOAT" -> CatalogType.FLOAT;
            case "DOUBLE" -> CatalogType.DOUBLE;
            case "DECIMAL" -> null;
            case "BOOLEAN", "BOOL" -> CatalogType.BOOLEAN;
            case "DATE" -> CatalogType.DATE;
            case "TIMESTAMP" -> CatalogType.TIMESTAMP;
            case "VARCHAR", "STRING", "TEXT" -> CatalogType.VARCHAR;
            default -> CatalogType.VARCHAR;
        };
    }
}