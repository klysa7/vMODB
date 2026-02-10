package dk.ku.di.dms.vms.calcite.http;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import dk.ku.di.dms.vms.calcite.service.OlapGatewayService;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.ERROR;

public final class GatewayHttpHandler implements HttpHandler {

    private static final System.Logger LOGGER = System.getLogger(GatewayHttpHandler.class.getName());

    static final String PATH_ORDERS = "/olap/orders";

    static final String SQL = """
        SELECT o.customer_id, o.order_id, pc.sequential
        FROM "order".orders o
        JOIN payment.order_payment_cards pc
          ON o.customer_id = pc.customer_id
        """;

    private final OlapGatewayService service;

    public GatewayHttpHandler(OlapGatewayService service) {
        this.service = service;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String path = exchange.getRequestURI().getPath();

        LOGGER.log(INFO, "Received Request: " + method + " " + path);

        if (!"GET".equalsIgnoreCase(method)) {
            send(exchange, 405, jsonError("Method not allowed. Use GET."));
            return;
        }

        if (!PATH_ORDERS.equals(path)) {
            send(exchange, 404, jsonError("Unknown endpoint. Use " + PATH_ORDERS));
            return;
        }

        try {
            String responseJson = service.execute(SQL);
            send(exchange, 200, responseJson);
        } catch (Exception e) {
            LOGGER.log(ERROR, "Gateway Error", e);
            send(exchange, 500, jsonError("Gateway error: " + e.getMessage()));
        }
    }

    private static void send(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static String jsonError(String msg) {
        return "{"
                + "\"status\":\"error\","
                + "\"message\":" + "\"" + msg.replace("\"", "\\\"") + "\""
                + "}";
    }
}