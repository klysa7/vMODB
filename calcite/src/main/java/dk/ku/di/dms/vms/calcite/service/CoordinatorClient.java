package dk.ku.di.dms.vms.calcite.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.exception.CoordinatorClientException;
import dk.ku.di.dms.vms.modb.common.coordinator.api.CatalogResponse;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
/**
 * HTTP client for the vMODB coordinator's internal REST API.
 * Currently exposes one endpoint: {@code /internal/catalog}, which
 * returns the full schema catalog consumed by {@link OlapGatewayService}
 * to build the Calcite schema and query planner on first use.
 * Wraps all HTTP and JSON errors in {@link CoordinatorClientException}.
 */
public final class CoordinatorClient {

    private final ObjectMapper mapper;
    private final HttpClient http;
    private final String baseUrl;

    public CoordinatorClient(String baseUrl) {
        this(baseUrl, HttpClient.newHttpClient(), new ObjectMapper());
    }

    public CoordinatorClient(String baseUrl, HttpClient http, ObjectMapper mapper) {
        this.baseUrl = stripTrailingSlash(baseUrl);
        this.http = http;
        this.mapper = mapper;
    }

    public CatalogResponse getCatalog() {
        return getJson("/internal/catalog", CatalogResponse.class);
    }

    private <T> T getJson(String path, Class<T> clazz) {
        String json = get(path);
        try {
            return mapper.readValue(json, clazz);
        } catch (Exception e) {
            throw new CoordinatorClientException("Failed to parse coordinator response", baseUrl + path, 400, json, e);
        }
    }

    private String get(String path) {
        String url = baseUrl + path;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .GET()
                    .build();

            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new CoordinatorClientException("Coordinator returned error", url, response.statusCode(), response.body(), null);
            }
            return response.body();
        } catch (CoordinatorClientException e) {
            throw e;
        } catch (Exception e) {
            throw new CoordinatorClientException("Failed calling coordinator", url, -1, null, e);
        }
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}