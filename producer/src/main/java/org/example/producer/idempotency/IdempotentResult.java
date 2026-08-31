package org.example.producer.idempotency;

import java.util.UUID;

public record IdempotentResult(UUID eventId, boolean replayed) {
}
