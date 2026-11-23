package com.campuslab.ms_campuslab_admin.kafka;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Reglas simples para no tocar tópicos internos de Kafka. */
public final class TopicRules {

    private TopicRules() {
    }

    public static boolean isInternal(String name) {
        return name != null && name.startsWith("__");
    }

    public static void assertManageable(String name) {
        if (isInternal(name)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN,
                    "Los tópicos internos de Kafka (prefijo __) no se pueden modificar");
        }
    }
}
