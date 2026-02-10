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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class VmsHttpClient {

    private static final System.Logger LOGGER =
            System.getLogger(VmsHttpClient.class.getName());
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    public static final String SNAPSHOT_HEADER = "X-VMODB-SNAPSHOT";

    public PushdownResponse executeScanAll(VmsSubplan sp, long snapshot) {
        try {
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(sp.url))
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header(SNAPSHOT_HEADER, Long.toString(snapshot))
                    .GET()
                    .build();

            LOGGER.log(System.Logger.Level.INFO,
                    "Sending Request to [" + sp.vmsName + "] Snapshot: " + snapshot + " URL: " + sp.url);

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());

            String bodyRaw = resp.body() == null ? "" : resp.body();
            String body = bodyRaw.trim();

            if (!(body.startsWith("[") || body.startsWith("{"))) {
                throw new RuntimeException("Non-JSON response from " + sp.vmsName + ": " + body);
            }

            JsonNode root = mapper.readTree(body);
            if (!root.isArray()) {
                throw new RuntimeException("Expected JSON array from " + sp.vmsName);
            }

            List<List<Object>> rows = new ArrayList<>();

            Set<String> missingColsLogged = new HashSet<>();

            root.forEach(obj -> {
                if (!obj.isObject()) return;

                List<Object> row = new ArrayList<>(sp.columnsInOrder.size());

                for (String col : sp.columnsInOrder) {
                    JsonNode valNode = obj.get(col);

                    if (valNode == null) {
                        // Log only the first time we see this missing column
                        if (!missingColsLogged.contains(col)) {
                            LOGGER.log(System.Logger.Level.WARNING,
                                    "VMS [" + sp.vmsName + "] missing column: '" + col + "' (Inserting NULL). Suppressing further warnings.");
                            missingColsLogged.add(col);
                        }
                        row.add(null);
                    } else {
                        row.add(jsonToJava(valNode));
                    }
                }
                rows.add(row);
            });

            LOGGER.log(System.Logger.Level.INFO,
                    "VMS [" + sp.vmsName + "] returned " + rows.size() + " rows.");

            return new PushdownResponse(sp.vmsName, snapshot, sp.columnsInOrder, rows);

        } catch (Exception e) {
            throw new RuntimeException("Failed executing subplan at " + sp.url + " for " + sp.vmsName, e);
        }
    }

    private Object jsonToJava(JsonNode v) {
        if (v == null || v.isNull()) return null;
        // Important: Force everything to LONG to ensure joins work with Integers
        if (v.isIntegralNumber()) return v.longValue();
        if (v.isFloatingPointNumber()) return v.doubleValue();
        if (v.isBoolean()) return v.booleanValue();
        if (v.isTextual()) return v.textValue();
        return v.toString();
    }
}