package dk.ku.di.dms.vms.coordinator.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dk.ku.di.dms.vms.coordinator.olap.queryPlanner.catalog.CoordinatorCatalog;
import dk.ku.di.dms.vms.modb.common.coordinator.api.*;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class CoordinatorInternalApi {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CoordinatorCatalog catalog;
    private final HttpServer server;

    private CoordinatorInternalApi(CoordinatorCatalog catalog, HttpServer server) {
        this.catalog = catalog;
        this.server = server;
    }

    public static CoordinatorInternalApi start(CoordinatorCatalog catalog, String host, int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        CoordinatorInternalApi api = new CoordinatorInternalApi(catalog, server);

        server.createContext("/internal/snapshot", api::handleSnapshot);
        server.createContext("/internal/catalog",  api::handleCatalog);

        server.setExecutor(null);
        server.start();

        System.err.println("Coordinator Internal API started on " + host + ":" + port);
        return api;
    }

    private void handleSnapshot(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, Map.of("status", "error", "message", "Method not allowed. Use GET."));
            return;
        }
        sendJson(ex, 200, new SnapshotResponse(catalog.getSnapshotId()));
    }

    private void handleCatalog(HttpExchange ex) throws IOException {
        if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
            sendJson(ex, 405, Map.of("status", "error", "message", "Method not allowed. Use GET."));
            return;
        }

        long snapshotId = catalog.getSnapshotId();
        Map<String, Map<String, CatalogTableDto>> schemasDto = new HashMap<>();

        for (String schema : catalog.schemaNames()) {
            Map<String, CatalogTableDto> tablesDto = new HashMap<>();

            for (var entry : catalog.tablesInSchema(schema).entrySet()) {
                String tableName = entry.getKey();
                Object tableDef  = entry.getValue();

                String owner            = catalog.ownerOf(schema, tableName);
                List<CatalogColumnDto> cols = extractColumns(tableDef);

                tablesDto.put(tableName, new CatalogTableDto(owner, cols));
            }

            schemasDto.put(schema, tablesDto);
        }

        List<PlacementDto> placement = new ArrayList<>();
        for (String schema : catalog.schemaNames()) {
            for (String table : catalog.tablesInSchema(schema).keySet()) {
                placement.add(new PlacementDto(schema, table, catalog.ownerOf(schema, table)));
            }
        }

        sendJson(ex, 200, new CatalogResponse(snapshotId, schemasDto, placement));
    }

    private static void sendJson(HttpExchange ex, int code, Object obj) throws IOException {
        byte[] bytes = MAPPER.writeValueAsBytes(obj);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
        ex.close();
    }

    /**
     * Extracts column descriptors from a coordinator CatalogTable object via reflection.
     *
     * For each column we extract:
     *   name     — via name() or getName()
     *   type     — via type() or getType(), returned as the enum's toString()
     *   byteSize — via byteSize() or getByteSize().
     *             This is the exact byte width the column occupies in the VMS off-heap layout
     *             (= DataType.value for that column's type in the VMS Schema).
     *             For fixed-width numeric types (INT=4, LONG=8, FLOAT=4, DOUBLE=8, BOOL=1)
     *             the gateway can derive the size from the type alone, so 0 is a safe fallback.
     *             For VARCHAR/CHAR columns the gateway CANNOT derive the size from the type —
     *             it must come from here. A 0 here means VmsResultIterator will output null
     *             for that column rather than the real string value.
     */
    private static List<CatalogColumnDto> extractColumns(Object catalogTable) {
        if (catalogTable == null) return List.of();

        Object cols = tryInvoke(catalogTable, "columns");
        if (cols == null) cols = tryInvoke(catalogTable, "getColumns");
        if (!(cols instanceof List<?> list)) return List.of();

        List<CatalogColumnDto> out = new ArrayList<>(list.size());
        for (Object col : list) {
            if (col == null) continue;

            String name = sanitize(asString(
                    tryInvoke(col, "name"),
                    tryInvoke(col, "getName")
            ));

            String type = sanitize(asString(
                    tryInvoke(col, "type"),
                    tryInvoke(col, "getType")
            ));

            // byteSize: try byteSize() first (our updated CatalogColumn record method),
            // then getByteSize() for any legacy bean-style accessor.
            // Falls back to 0 if neither exists — safe for numeric types, produces null
            // output for VARCHAR columns until the coordinator CatalogColumn is updated.
            int byteSize = asInt(
                    tryInvoke(col, "byteSize"),
                    tryInvoke(col, "getByteSize")
            );

            out.add(new CatalogColumnDto(name, type, byteSize));
        }
        return out;
    }

    private static String sanitize(String s) {
        if (s == null) return null;
        return s.replace("\r", "").replace("\n", "").trim();
    }

    private static Object tryInvoke(Object target, String method) {
        try {
            var m = target.getClass().getMethod(method);
            m.setAccessible(true);
            return m.invoke(target);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String asString(Object... vals) {
        for (Object v : vals) {
            if (v != null) return String.valueOf(v);
        }
        return null;
    }

    /**
     * Converts the first non-null value to int. Returns 0 if all values are null
     * or cannot be parsed as an integer.
     */
    private static int asInt(Object... vals) {
        for (Object v : vals) {
            if (v instanceof Integer i) return i;
            if (v instanceof Number  n) return n.intValue();
            if (v instanceof String  s) {
                try { return Integer.parseInt(s.trim()); } catch (NumberFormatException ignored) {}
            }
        }
        return 0;
    }
}