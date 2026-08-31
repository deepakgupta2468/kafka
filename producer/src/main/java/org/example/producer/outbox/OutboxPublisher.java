package org.example.producer.outbox;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.time.Instant;
import io.micrometer.core.instrument.MeterRegistry;
import org.example.events.EmployeeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OutboxPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisher.class);
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;
    private final MeterRegistry meterRegistry;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper,
            KafkaTemplate<String, EmployeeEvent> kafkaTemplate,
            MeterRegistry meterRegistry
    ) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(fixedDelayString = "${app.outbox.publish-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        for (OutboxEventEntity outboxEvent : outboxEventRepository.findTop100ByPublishedAtIsNullOrderByCreatedAtAsc()) {
            try {
                EmployeeEvent event = objectMapper.readValue(outboxEvent.getPayload(), EmployeeEvent.class);
                kafkaTemplate.send(outboxEvent.getTopic(), outboxEvent.getAggregateKey(), event).get();
                outboxEvent.markPublished(Instant.now());
                meterRegistry.counter("employee.outbox.events", "outcome", "published").increment();
                LOGGER.info("Published outbox event: eventId={}, employeeId={}", event.eventId(), event.employeeId());
            }
            catch (JacksonException exception) {
                meterRegistry.counter("employee.outbox.events", "outcome", "failed").increment();
                throw new IllegalStateException("Unable to deserialize outbox event " + outboxEvent.getId(), exception);
            }
            catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while publishing outbox event " + outboxEvent.getId(), exception);
            }
            catch (Exception exception) {
                meterRegistry.counter("employee.outbox.events", "outcome", "failed").increment();
                throw new IllegalStateException("Unable to publish outbox event " + outboxEvent.getId(), exception);
            }
        }
    }
}
