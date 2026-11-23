package com.campuslab.ms_campuslab_admin.rabbit;

import java.util.Map;

import jakarta.validation.constraints.NotBlank;

public final class RabbitDtos {

    private RabbitDtos() {
    }

    /** arguments admite, por ejemplo, x-dead-letter-exchange y x-dead-letter-routing-key para una DLQ. */
    public record QueueRequest(@NotBlank String name, Boolean durable, Map<String, Object> arguments) {
    }

    /** type: direct | topic | fanout | headers */
    public record ExchangeRequest(@NotBlank String name, @NotBlank String type, Boolean durable) {
    }

    /** destinationType: QUEUE (por defecto) o EXCHANGE. */
    public record BindingRequest(@NotBlank String source, @NotBlank String destination,
                                 String destinationType, String routingKey) {
    }
}
