package org.example.consumer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "dead_letter_events")
public class DeadLetterEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private UUID eventId;

    private Long employeeId;

    @Column(nullable = false)
    private Instant receivedAt;

    @Lob
    private String failureReason;

    @Lob
    private String rawPayload;

    protected DeadLetterEventEntity() {
    }

    public DeadLetterEventEntity(
            UUID eventId,
            Long employeeId,
            Instant receivedAt,
            String failureReason,
            String rawPayload
    ) {
        this.eventId = eventId;
        this.employeeId = employeeId;
        this.receivedAt = receivedAt;
        this.failureReason = failureReason;
        this.rawPayload = rawPayload;
    }
}
