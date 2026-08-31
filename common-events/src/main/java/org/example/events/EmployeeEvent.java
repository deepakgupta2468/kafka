package org.example.events;

import java.time.Instant;
import java.util.UUID;

public record EmployeeEvent(
        UUID eventId,
        int schemaVersion,
        EmployeeEventType type,
        Long employeeId,
        String name,
        String department,
        Instant occurredAt
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
