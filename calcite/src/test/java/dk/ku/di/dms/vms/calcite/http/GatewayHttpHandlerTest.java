package dk.ku.di.dms.vms.calcite.http;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GatewayHttpHandlerTest {

    @Test
    void invalidGet() throws Exception {
        OlapGatewayService olapGatewayService = mock(OlapGatewayService.class);
        GatewayHttpHandler gatewayHttpHandler = new GatewayHttpHandler(olapGatewayService);

        FakeExchange fakeExchange = new FakeExchange("POST", "/olap/orders");
        gatewayHttpHandler.handle(fakeExchange.exchange);

        assertThat(fakeExchange.statusCode).isEqualTo(405);
        assertThat(fakeExchange.responseBodyUtf8()).contains("\"status\":\"error\"");
        assertThat(fakeExchange.responseBodyUtf8()).contains("Method not allowed");
        assertThat(fakeExchange.headers.getFirst("Content-Type")).contains("application/json");

        verifyNoInteractions(olapGatewayService);
    }

    @Test
    void invalidPath() throws Exception {
        OlapGatewayService olapGatewayService = mock(OlapGatewayService.class);
        GatewayHttpHandler gatewayHttpHandler = new GatewayHttpHandler(olapGatewayService);

        FakeExchange fakeExchange = new FakeExchange("GET", "/nope");
        gatewayHttpHandler.handle(fakeExchange.exchange);

        assertThat(fakeExchange.statusCode).isEqualTo(404);
        assertThat(fakeExchange.responseBodyUtf8()).contains("Unknown endpoint");
        verifyNoInteractions(olapGatewayService);
    }

    @Test
    void success() throws Exception {
        OlapGatewayService olapGatewayService = mock(OlapGatewayService.class);
        when(olapGatewayService.execute(anyString())).thenReturn("{\"status\":\"ok\"}");

        GatewayHttpHandler gatewayHttpHandler = new GatewayHttpHandler(olapGatewayService);
        FakeExchange fakeExchange = new FakeExchange("GET", "/olap/orders");

        gatewayHttpHandler.handle(fakeExchange.exchange);

        assertThat(fakeExchange.statusCode).isEqualTo(200);
        assertThat(fakeExchange.responseBodyUtf8()).isEqualTo("{\"status\":\"ok\"}");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(olapGatewayService).execute(sqlCaptor.capture());
        assertThat(sqlCaptor.getValue().trim()).isEqualTo(GatewayHttpHandler.SQL.trim());
    }

    @Test
    void serviceThrows_returns500WithGatewayError() throws Exception {
        OlapGatewayService olapGatewayService = mock(OlapGatewayService.class);
        when(olapGatewayService.execute(anyString())).thenThrow(new RuntimeException("exception"));

        GatewayHttpHandler gatewayHttpHandler = new GatewayHttpHandler(olapGatewayService);
        FakeExchange fakeExchange = new FakeExchange("GET", "/olap/orders");

        gatewayHttpHandler.handle(fakeExchange.exchange);

        assertThat(fakeExchange.statusCode).isEqualTo(500);
        assertThat(fakeExchange.responseBodyUtf8()).contains("No Gateway");
    }


    static final class FakeExchange {
        HttpExchange exchange = mock(HttpExchange.class);
        Headers headers = new Headers();
        ByteArrayOutputStream body = new ByteArrayOutputStream();

        int statusCode = -1;
        long contentLength = -1;

        FakeExchange(String method, String path) throws Exception {
            when(exchange.getRequestMethod()).thenReturn(method);
            when(exchange.getRequestURI()).thenReturn(new URI("http://localhost" + path));
            when(exchange.getResponseHeaders()).thenReturn(headers);
            when(exchange.getResponseBody()).thenReturn(body);

            doAnswer(inv -> {
                statusCode = inv.getArgument(0, Integer.class);
                contentLength = inv.getArgument(1, Long.class);
                return null;
            }).when(exchange).sendResponseHeaders(anyInt(), anyLong());
        }

        String responseBodyUtf8() {
            return body.toString(StandardCharsets.UTF_8);
        }
    }
}