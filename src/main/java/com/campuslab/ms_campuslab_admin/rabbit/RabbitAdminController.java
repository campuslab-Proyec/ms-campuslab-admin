package com.campuslab.ms_campuslab_admin.rabbit;

import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.BindingRequest;
import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.ExchangeRequest;
import com.campuslab.ms_campuslab_admin.rabbit.RabbitDtos.QueueRequest;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/admin/rabbit")
public class RabbitAdminController {

    private final RabbitAdminService service;

    public RabbitAdminController(RabbitAdminService service) {
        this.service = service;
    }

    // ---- colas
    @GetMapping("/queues")
    public List<Map<String, Object>> listQueues() {
        return service.listQueues();
    }

    @PostMapping("/queues")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> createQueue(@Valid @RequestBody QueueRequest req) {
        service.createQueue(req);
        return Map.of("queue", req.name(), "status", "creada");
    }

    @DeleteMapping("/queues/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteQueue(@PathVariable String name) {
        service.deleteQueue(name);
    }

    // ---- exchanges
    @GetMapping("/exchanges")
    public List<Map<String, Object>> listExchanges() {
        return service.listExchanges();
    }

    @PostMapping("/exchanges")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> createExchange(@Valid @RequestBody ExchangeRequest req) {
        service.createExchange(req);
        return Map.of("exchange", req.name(), "status", "creado");
    }

    @DeleteMapping("/exchanges/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteExchange(@PathVariable String name) {
        service.deleteExchange(name);
    }

    // ---- bindings
    @GetMapping("/bindings")
    public List<Map<String, Object>> listBindings() {
        return service.listBindings();
    }

    @PostMapping("/bindings")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> createBinding(@Valid @RequestBody BindingRequest req) {
        service.createBinding(req);
        return Map.of("source", req.source(), "destination", req.destination(), "status", "creado");
    }

    /** Se elimina con el mismo cuerpo con el que se creó. */
    @PostMapping("/bindings/delete")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteBinding(@Valid @RequestBody BindingRequest req) {
        service.deleteBinding(req);
    }
}
