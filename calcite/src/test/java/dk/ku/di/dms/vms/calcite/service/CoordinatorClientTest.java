package dk.ku.di.dms.vms.calcite.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.ku.di.dms.vms.calcite.exception.CoordinatorClientException;
import dk.ku.di.dms.vms.modb.common.coordinator.api.SnapshotResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CoordinatorClientTest {

    @Mock HttpClient http;
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void getSnapshotAndCorrectUrl() throws Exception {
        CoordinatorClient client =
                new CoordinatorClient("http://localhost:8079/", http, mapper);

        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(200);
        when(httpResponse.body()).thenReturn("{\"snapshotId\":2}");

        HttpRequest httpRequest = mock(HttpRequest.class);
        HttpResponse.BodyHandler bodyHandler = mock(HttpResponse.BodyHandler.class);

        when(http.send(httpRequest, bodyHandler)).thenReturn(httpResponse);
        SnapshotResponse snapshotResponse = client.getSnapshot();

        ArgumentCaptor<HttpRequest> reqCaptor = ArgumentCaptor.forClass(HttpRequest.class);

        verify(http).send(reqCaptor.capture(), bodyHandler);
        assertThat(reqCaptor.getValue().uri().toString())
                .isEqualTo("http://localhost:8079/internal/snapshot");
        assertThat(snapshotResponse.snapshotId()).isEqualTo(2);
    }

    @Test
    void getCatalogFailed() throws Exception {
        CoordinatorClient client =
                new CoordinatorClient("http://localhost:8079", http, mapper);

        HttpResponse<String> httpResponse = mock(HttpResponse.class);
        when(httpResponse.statusCode()).thenReturn(503);
        when(httpResponse.body()).thenReturn("No Service");

        HttpRequest httpRequest = mock(HttpRequest.class);
        HttpResponse.BodyHandler bodyHandler = mock(HttpResponse.BodyHandler.class);

        when(http.send(httpRequest, bodyHandler)).thenReturn(httpResponse);

        assertThatThrownBy(client::getCatalog)
                .isInstanceOf(CoordinatorClientException.class)
                .satisfies(exception -> {
                    CoordinatorClientException cex = (CoordinatorClientException) exception;
                    assertThat(cex.statusCode).isEqualTo(503);
                    assertThat(cex.body).isEqualTo("No Service");
                    assertThat(cex.url).endsWith("/internal/catalog");
                });
    }

    @Test
    void getSnapshotFailedBeforeResponse() throws Exception {
        CoordinatorClient client =
                new CoordinatorClient("http://localhost:8079", http, mapper);

        HttpRequest httpRequest = mock(HttpRequest.class);
        HttpResponse.BodyHandler bodyHandler = mock(HttpResponse.BodyHandler.class);

        when(http.send(httpRequest, bodyHandler)).thenThrow(new IOException("Error"));

        assertThatThrownBy(client::getSnapshot)
                .isInstanceOf(CoordinatorClientException.class)
                .satisfies(exception -> {
                    CoordinatorClientException cex = (CoordinatorClientException) exception;
                    assertThat(cex.statusCode).isEqualTo(-1);
                    assertThat(cex.body).isNull();
                    assertThat(cex.url).endsWith("/internal/snapshot");
                });
    }
}