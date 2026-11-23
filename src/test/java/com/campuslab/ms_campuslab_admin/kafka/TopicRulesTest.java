package com.campuslab.ms_campuslab_admin.kafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class TopicRulesTest {

    @Test
    void internalTopicsAreProtected() {
        assertTrue(TopicRules.isInternal("__consumer_offsets"));
        assertThrows(ResponseStatusException.class, () -> TopicRules.assertManageable("__transaction_state"));
    }

    @Test
    void normalTopicsAreManageable() {
        assertFalse(TopicRules.isInternal("campuslab.bookings.events"));
        assertDoesNotThrow(() -> TopicRules.assertManageable("campuslab.bookings.events"));
    }
}
