package org.example.consumer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
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

    @Column(columnDefinition = "TEXT")
    private String failureReason;

    @Column(columnDefinition = "TEXT")
    private String rawPayload;

    private String sourceTopic;

    private Integer sourcePartition;

    private Long sourceOffset;

    @Column(columnDefinition = "TEXT")
    private String exceptionMessage;

    protected DeadLetterEventEntity() {
    }

    public DeadLetterEventEntity(
            UUID eventId,
            Long employeeId,
            Instant receivedAt,
            String failureReason,
            String rawPayload,
            String sourceTopic,
            Integer sourcePartition,
            Long sourceOffset,
            String exceptionMessage
    ) {
        this.eventId = eventId;
        this.employeeId = employeeId;
        this.receivedAt = receivedAt;
        this.failureReason = failureReason;
        this.rawPayload = rawPayload;
        this.sourceTopic = sourceTopic;
        this.sourcePartition = sourcePartition;
        this.sourceOffset = sourceOffset;
        this.exceptionMessage = exceptionMessage;
    }
}
