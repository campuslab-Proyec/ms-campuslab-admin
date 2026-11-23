package com.campuslab.ms_campuslab_admin.rabbit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.HeadersExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.server.ResponseStatusException;

import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.BindingRequest;
import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.ExchangeRequest;
import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.QueueRequest;

/**
 * Crear/eliminar usa AmqpAdmin (protocolo AMQP). Listar usa la API HTTP de gestión de RabbitMQ (puerto 15672).
 */
@Service
public class RabbitAdminService {

    private static final ParameterizedTypeReference<List<Map<String, Object>>> LIST_OF_MAPS =
            new ParameterizedTypeReference<>() {
            };

    private final AmqpAdmin amqp;
    private final RestClient management;

    public RabbitAdminService(AmqpAdmin amqp,
                              @Value("${rabbitmq.management.url}") String managementUrl,
                              @Value("${spring.rabbitmq.username}") String username,
                              @Value("${spring.rabbitmq.password}") String password) {
        this.amqp = amqp;
        this.management = RestClient.builder()
                .baseUrl(managementUrl)
                .defaultHeaders(h -> h.setBasicAuth(username, password))
                .build();
    }

    // ------------------------------------------------------------------ colas

    public List<Map<String, Object>> listQueues() {
        return fetch("/api/queues").stream()
                .map(q -> pick(q, "name", "vhost", "durable", "messages", "consumers", "arguments"))
                .toList();
    }

    public void createQueue(QueueRequest req) {
        boolean durable = req.durable() == null || req.durable();
        Map<String, Object> args = req.arguments() == null ? Map.of() : req.arguments();
        amqpCall(() -> amqp.declareQueue(new Queue(req.name(), durable, false, false, args)));
    }

    public void deleteQueue(String name) {
        Boolean deleted = amqpCall(() -> amqp.deleteQueue(name));
        if (!Boolean.TRUE.equals(deleted)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "La cola no existe: " + name);
        }
    }

    // -------------------------------------------------------------- exchanges

    public List<Map<String, Object>> listExchanges() {
        return fetch("/api/exchanges").stream()
                .filter(e -> !String.valueOf(e.get("name")).isEmpty())
                .map(e -> pick(e, "name", "vhost", "type", "durable"))
                .toList();
    }

    public void createExchange(ExchangeRequest req) {
        boolean durable = req.durable() == null || req.durable();
        Exchange exchange = switch (req.type().toLowerCase()) {
            case "direct" -> new DirectExchange(req.name(), durable, false);
            case "topic" -> new TopicExchange(req.name(), durable, false);
            case "fanout" -> new FanoutExchange(req.name(), durable, false);
            case "headers" -> new HeadersExchange(req.name(), durable, false);
            default -> throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Tipo de exchange inválido: use direct, topic, fanout o headers");
        };
        amqpCall(() -> {
            amqp.declareExchange(exchange);
            return null;
        });
    }

    public void deleteExchange(String name) {
        if (name.startsWith("amq.")) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "No se pueden eliminar exchanges del sistema");
        }
        Boolean deleted = amqpCall(() -> amqp.deleteExchange(name));
        if (!Boolean.TRUE.equals(deleted)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "El exchange no existe: " + name);
        }
    }

    // --------------------------------------------------------------- bindings

    public List<Map<String, Object>> listBindings() {
        return fetch("/api/bindings").stream()
                .filter(b -> !String.valueOf(b.get("source")).isEmpty())
                .map(b -> pick(b, "source", "destination", "destination_type", "routing_key", "vhost"))
                .toList();
    }

    public void createBinding(BindingRequest req) {
        Binding binding = toBinding(req);
        amqpCall(() -> {
            amqp.declareBinding(binding);
            return null;
        });
    }

    public void deleteBinding(BindingRequest req) {
        Binding binding = toBinding(req);
        amqpCall(() -> {
            amqp.removeBinding(binding);
            return null;
        });
    }

    private Binding toBinding(BindingRequest req) {
        Binding.DestinationType type;
        try {
            type = req.destinationType() == null
                    ? Binding.DestinationType.QUEUE
                    : Binding.DestinationType.valueOf(req.destinationType().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "destinationType debe ser QUEUE o EXCHANGE");
        }
        String routingKey = req.routingKey() == null ? "" : req.routingKey();
        return new Binding(req.destination(), type, req.source(), routingKey, null);
    }

    // ---------------------------------------------------------------- helpers

    private List<Map<String, Object>> fetch(String path) {
        try {
            List<Map<String, Object>> body = management.get().uri(path).retrieve().body(LIST_OF_MAPS);
            return body == null ? List.of() : body;
        } catch (RestClientException e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,
                    "No se pudo consultar la API de gestión de RabbitMQ: " + e.getMessage());
        }
    }

    private <T> T amqpCall(java.util.function.Supplier<T> action) {
        try {
            return action.get();
        } catch (AmqpException e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Error de RabbitMQ: " + e.getMessage());
        }
    }

    private static Map<String, Object> pick(Map<String, Object> source, String... keys) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String k : keys) {
            result.put(k, source.get(k));
        }
        return result;
    }
}
