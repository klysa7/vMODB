package dk.ku.di.dms.vms.calcite.olap.orchestrator.rpc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.planning.VmsSubplan;
import dk.ku.di.dms.vms.calcite.olap.orchestrator.runtime.PushdownResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public final class VmsHttpClient {

    private static final System.Logger LOGGER =
            System.getLogger(VmsHttpClient.class.getName());
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private static final int RAW_PREVIEW_CHARS = 12000;
    private static final int ROW_PREVIEW_COUNT = 3;
    public static final String SNAPSHOT_HEADER = "X-VMODB-SNAPSHOT";

    private static String preview(String s, int maxChars) {
        if (s == null) return "";
        if (s.length() <= maxChars) return s;
        return s.substring(0, maxChars) + "...(truncated)";
    }

    public PushdownResponse executeScanAll(VmsSubplan sp, long snapshot) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(sp.url))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header(SNAPSHOT_HEADER, Long.toString(snapshot))
                    .GET()
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

            String bodyRaw = resp.body() == null ? "" : resp.body();

            String body = bodyRaw.trim();
            if (!(body.startsWith("[") || body.startsWith("{"))) {
                throw new RuntimeException("Non-JSON response from ");
            }

            JsonNode root = mapper.readTree(body);
            if (!root.isArray()) {
                throw new RuntimeException("Expected JSON array from ");
            }

            List<List<Object>> rows = new ArrayList<>();
            root.forEach(obj -> {
                if (!obj.isObject()) return; // same as continue

                List<Object> row = new ArrayList<>(sp.columnsInOrder.size());
                sp.columnsInOrder.forEach(col ->
                        row.add(jsonToJava(obj.get(col)))
                );
                rows.add(row);
            });

            if (!rows.isEmpty()) {
                LOGGER.log(System.Logger.Level.DEBUG,
                        "First row (" + sp.vmsName + "): " + rows.get(0));
            }

            return new PushdownResponse(sp.vmsName, snapshot, sp.columnsInOrder, rows);

        } catch (Exception e) {
            throw new RuntimeException("Failed executing subplan at " + sp.url + " for " + sp.vmsName, e);
        }
    }

    private Object jsonToJava(JsonNode v) {
        if (v == null || v.isNull()) return null;
        if (v.isIntegralNumber()) return v.longValue();
        if (v.isFloatingPointNumber()) return v.doubleValue();
        if (v.isBoolean()) return v.booleanValue();
        if (v.isTextual()) return v.textValue();
        return v.toString();
    }
}