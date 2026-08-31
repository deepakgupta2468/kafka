package org.example.producer.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String aggregateKey;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(nullable = false)
    private int retryCount;

    @Column(columnDefinition = "TEXT")
    private String lastError;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;

    protected OutboxEventEntity() {
    }

    public OutboxEventEntity(UUID id, String aggregateKey, String topic, String payload, Instant createdAt) {
        this.id = id;
        this.aggregateKey = aggregateKey;
        this.topic = topic;
        this.payload = payload;
        this.status = OutboxStatus.PENDING;
        this.retryCount = 0;
        this.createdAt = createdAt;
    }

    public UUID getId() {
        return id;
    }

    public String getAggregateKey() {
        return aggregateKey;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void markPublishing() {
        this.status = OutboxStatus.PUBLISHING;
    }

    public void markPublished(Instant publishedAt) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedAt;
        this.lastError = null;
    }

    public void markPendingAfterFailure(String error) {
        this.status = OutboxStatus.PENDING;
        this.retryCount++;
        this.lastError = error;
    }

    public void markFailed(String error) {
        this.status = OutboxStatus.FAILED;
        this.lastError = error;
    }
}
