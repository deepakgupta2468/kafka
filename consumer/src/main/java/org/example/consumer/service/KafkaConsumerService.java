package org.example.consumer.service;

import java.time.Instant;
import java.util.Base64;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.example.consumer.config.KafkaConsumerProperties;
import org.example.consumer.model.DeadLetterEventEntity;
import org.example.consumer.model.EmployeeEntity;
import org.example.consumer.model.ProcessedEventEntity;
import org.example.consumer.repository.DeadLetterEventRepository;
import org.example.consumer.repository.EmployeeRepository;
import org.example.consumer.repository.ProcessedEventRepository;
import org.example.events.EmployeeEvent;
import org.example.events.EmployeeEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class KafkaConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final KafkaConsumerProperties kafkaProperties;
    private final EmployeeRepository employeeRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final MeterRegistry meterRegistry;

    public KafkaConsumerService(
            KafkaConsumerProperties kafkaProperties,
            EmployeeRepository employeeRepository,
            ProcessedEventRepository processedEventRepository,
            DeadLetterEventRepository deadLetterEventRepository,
            MeterRegistry meterRegistry
    ) {
        this.kafkaProperties = kafkaProperties;
        this.employeeRepository = employeeRepository;
        this.processedEventRepository = processedEventRepository;
        this.deadLetterEventRepository = deadLetterEventRepository;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(
            topics = "${app.kafka.employee-topic}",
            groupId = "${app.kafka.consumer-group-id}",
            concurrency = "${app.kafka.concurrency}"
    )
    @Transactional
    public void consume(EmployeeEvent event) {
        if (event.eventId() == null || event.type() == null || event.employeeId() == null || event.occurredAt() == null) {
            throw new IllegalArgumentException("Employee event must contain an ID, type, employee ID, and timestamp");
        }
        if (event.schemaVersion() != EmployeeEvent.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported employee event schema version: " + event.schemaVersion());
        }
        if (processedEventRepository.existsById(event.eventId())) {
            meterRegistry.counter("employee.consumer.events", "outcome", "duplicate").increment();
            LOGGER.info("Ignoring duplicate employee event: eventId={}", event.eventId());
            return;
        }
        if (event.type() == EmployeeEventType.CREATED) {
            if (event.name() == null || event.department() == null) {
                throw new IllegalArgumentException("Employee creation event must contain name and department");
            }
            employeeRepository.save(EmployeeEntity.from(event));
            meterRegistry.counter("employee.consumer.events", "outcome", "created").increment();
            LOGGER.info("Persisted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
        }
        else if (event.type() == EmployeeEventType.DELETED) {
            employeeRepository.deleteById(event.employeeId());
            meterRegistry.counter("employee.consumer.events", "outcome", "deleted").increment();
            LOGGER.info("Deleted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
        }
        processedEventRepository.save(new ProcessedEventEntity(event.eventId(), Instant.now()));
    }

    @KafkaListener(
            topics = "${app.kafka.employee-topic}.dlt",
            groupId = "${app.kafka.dlt-consumer-group-id}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        String rawPayload = record.value() == null ? null : Base64.getEncoder().encodeToString(record.value());
        deadLetterEventRepository.save(new DeadLetterEventEntity(
                null,
                null,
                Instant.now(),
                "See Kafka dead-letter headers for failure details",
                rawPayload
        ));
        meterRegistry.counter("employee.consumer.events", "outcome", "dead_lettered").increment();
        LOGGER.error("Recorded dead-letter event: topic={}, partition={}, offset={}",
                record.topic(), record.partition(), record.offset());
    }
}
