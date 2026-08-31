package org.example.producer.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKeyEntity {

    @Id
    @Column(name = "idempotency_key")
    private String idempotencyKey;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false)
    private Instant createdAt;

    protected IdempotencyKeyEntity() {
    }

    public IdempotencyKeyEntity(String idempotencyKey, UUID eventId, Instant createdAt) {
        this.idempotencyKey = idempotencyKey;
        this.eventId = eventId;
        this.createdAt = createdAt;
    }

    public UUID getEventId() {
        return eventId;
    }
}
