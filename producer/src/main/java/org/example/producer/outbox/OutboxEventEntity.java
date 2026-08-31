package org.example.producer.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "outbox_events", indexes = @Index(name = "idx_outbox_events_published_at", columnList = "published_at"))
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String aggregateKey;

    @Column(nullable = false)
    private String topic;

    @Lob
    @Column(nullable = false)
    private String payload;

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

    public void markPublished(Instant publishedAt) {
        this.publishedAt = publishedAt;
    }
}
