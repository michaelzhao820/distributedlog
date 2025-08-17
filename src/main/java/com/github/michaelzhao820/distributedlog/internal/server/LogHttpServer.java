package com.github.michaelzhao820.distributedlog.internal.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LogHttpServer {

    private static final Logger logger = Logger.getLogger(LogHttpServer.class.getName());

    public record ProduceRequest(Log.Record record) {}
    public record ProduceResponse(long offset) {}
    public record ConsumeRequest(long offset) {}
    public record ConsumeResponse(Log.Record record) {}
    public record ErrorResponse(String error) {}

    private final Log log;
    private final HttpServer server;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LogHttpServer(Log log, String host, int port) throws IOException {
        this.log = log;
        this.server = HttpServer.create(new InetSocketAddress(host, port), 0);

        server.createContext("/produce", exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJsonResponse(exchange, new ErrorResponse("Method Not Allowed"), 405);
                return;
            }
            try {
                var req = objectMapper.readValue(exchange.getRequestBody(), ProduceRequest.class);
                long offset = this.log.append(req.record().value());
                sendJsonResponse(exchange, new ProduceResponse(offset), 200);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Bad request", e);
                sendJsonResponse(exchange, new ErrorResponse(e.getMessage()), 400);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Internal server error", e);
                sendJsonResponse(exchange, new ErrorResponse(e.getMessage()), 500);
            }
        });

        server.createContext("/consume", exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendJsonResponse(exchange, new ErrorResponse("Method Not Allowed"), 405);
                return;
            }
            try {
                var req = objectMapper.readValue(exchange.getRequestBody(), ConsumeRequest.class);
                Log.Record record = this.log.read(req.offset());
                sendJsonResponse(exchange, new ConsumeResponse(record), 200);
            } catch (IOException e) {
                logger.log(Level.WARNING, "Bad request", e);
                sendJsonResponse(exchange, new ErrorResponse(e.getMessage()), 400);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Internal server error", e);
                sendJsonResponse(exchange, new ErrorResponse(e.getMessage()), 500);
            }
        });
    }

    public void start() {
        server.start();
        logger.info("Log HTTP server started");
    }

    public void stop(int delaySeconds) {
        server.stop(delaySeconds);
        logger.info("Log HTTP server stopped");
    }

    private void sendJsonResponse(HttpExchange exchange, Object obj, int status) throws IOException {
        String json = objectMapper.writeValueAsString(obj);
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=UTF-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
