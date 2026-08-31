package org.example.producer.outbox;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.util.List;
import io.micrometer.core.instrument.MeterRegistry;
import org.example.events.EmployeeEvent;
import org.example.producer.config.OutboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxClaimService outboxClaimService;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;
    private final MeterRegistry meterRegistry;
    private final OutboxProperties outboxProperties;

    public OutboxPublisher(
            OutboxClaimService outboxClaimService,
            ObjectMapper objectMapper,
            KafkaTemplate<String, EmployeeEvent> kafkaTemplate,
            MeterRegistry meterRegistry,
            OutboxProperties outboxProperties
    ) {
        this.outboxClaimService = outboxClaimService;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
        this.outboxProperties = outboxProperties;
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish-interval-ms:1000}")
    public void publishPendingEvents() {
        List<OutboxEventEntity> claimedEvents = outboxClaimService.claimPendingEvents(outboxProperties.batchSize());
        for (OutboxEventEntity outboxEvent : claimedEvents) {
            publishClaimedEvent(outboxEvent);
        }
    }

    private void publishClaimedEvent(OutboxEventEntity outboxEvent) {
        try {
            EmployeeEvent event = objectMapper.readValue(outboxEvent.getPayload(), EmployeeEvent.class);
            kafkaTemplate.send(outboxEvent.getTopic(), outboxEvent.getAggregateKey(), event).get();
            outboxClaimService.markPublished(outboxEvent.getId());
            meterRegistry.counter("employee.outbox.events", "outcome", "published").increment();
            LOGGER.info("Published outbox event: eventId={}, employeeId={}", event.eventId(), event.employeeId());
        }
        catch (JacksonException exception) {
            String error = "Unable to deserialize outbox event: " + exception.getMessage();
            outboxClaimService.markPermanentFailure(outboxEvent.getId(), error);
            meterRegistry.counter("employee.outbox.events", "outcome", "failed").increment();
            LOGGER.error("Permanent outbox failure for event {}: {}", outboxEvent.getId(), error, exception);
        }
        catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            outboxClaimService.markRetryableFailure(outboxEvent.getId(), "Interrupted while publishing", outboxProperties.maxRetries());
            meterRegistry.counter("employee.outbox.events", "outcome", "failed").increment();
            LOGGER.warn("Interrupted while publishing outbox event {}", outboxEvent.getId(), exception);
        }
        catch (Exception exception) {
            String error = "Unable to publish outbox event: " + exception.getMessage();
            outboxClaimService.markRetryableFailure(outboxEvent.getId(), error, outboxProperties.maxRetries());
            meterRegistry.counter("employee.outbox.events", "outcome", "failed").increment();
            LOGGER.warn("Retryable outbox failure for event {}: {}", outboxEvent.getId(), error, exception);
        }
    }
}
