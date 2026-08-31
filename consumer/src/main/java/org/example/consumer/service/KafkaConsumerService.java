package org.example.consumer.service;

import tools.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.example.consumer.config.KafkaConsumerProperties;
import org.example.consumer.model.DeadLetterEventEntity;
import org.example.consumer.model.EmployeeEntity;
import org.example.consumer.repository.DeadLetterEventRepository;
import org.example.consumer.repository.EmployeeRepository;
import org.example.consumer.repository.ProcessedEventClaimRepository;
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
    private static final String DLT_EXCEPTION_MESSAGE = "kafka_dlt-exception-message";
    private static final String DLT_ORIGINAL_TOPIC = "kafka_dlt-original-topic";
    private static final String DLT_ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    private static final String DLT_ORIGINAL_OFFSET = "kafka_dlt-original-offset";

    private final KafkaConsumerProperties kafkaProperties;
    private final EmployeeRepository employeeRepository;
    private final ProcessedEventClaimRepository processedEventClaimRepository;
    private final DeadLetterEventRepository deadLetterEventRepository;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(
            KafkaConsumerProperties kafkaProperties,
            EmployeeRepository employeeRepository,
            ProcessedEventClaimRepository processedEventClaimRepository,
            DeadLetterEventRepository deadLetterEventRepository,
            MeterRegistry meterRegistry,
            ObjectMapper objectMapper
    ) {
        this.kafkaProperties = kafkaProperties;
        this.employeeRepository = employeeRepository;
        this.processedEventClaimRepository = processedEventClaimRepository;
        this.deadLetterEventRepository = deadLetterEventRepository;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${app.kafka.employee-topic}",
            groupId = "${app.kafka.consumer-group-id}",
            concurrency = "${app.kafka.concurrency}"
    )
    @Transactional
    public void consume(EmployeeEvent event) {
        validateEvent(event);
        if (claimEvent(event.eventId())) {
            meterRegistry.counter("employee.consumer.events", "outcome", "duplicate").increment();
            LOGGER.info("Ignoring duplicate employee event: eventId={}", event.eventId());
            return;
        }
        processEvent(event);
    }

    @KafkaListener(
            topics = "${app.kafka.employee-topic}.dlt",
            groupId = "${app.kafka.dlt-consumer-group-id}",
            containerFactory = "dltKafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeDeadLetter(ConsumerRecord<String, byte[]> record) {
        EmployeeEvent parsedEvent = parseEvent(record.value());
        String exceptionMessage = headerValue(record, DLT_EXCEPTION_MESSAGE);
        String sourceTopic = headerValue(record, DLT_ORIGINAL_TOPIC);
        Integer sourcePartition = parseIntegerHeader(record, DLT_ORIGINAL_PARTITION);
        Long sourceOffset = parseLongHeader(record, DLT_ORIGINAL_OFFSET);

        deadLetterEventRepository.save(new DeadLetterEventEntity(
                parsedEvent != null ? parsedEvent.eventId() : null,
                parsedEvent != null ? parsedEvent.employeeId() : null,
                Instant.now(),
                exceptionMessage != null ? exceptionMessage : "Unknown failure",
                record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8),
                sourceTopic != null ? sourceTopic : record.topic(),
                sourcePartition != null ? sourcePartition : record.partition(),
                sourceOffset != null ? sourceOffset : record.offset(),
                exceptionMessage
        ));
        meterRegistry.counter("employee.consumer.events", "outcome", "dead_lettered").increment();
        LOGGER.error(
                "Recorded dead-letter event: topic={}, partition={}, offset={}, eventId={}",
                record.topic(),
                record.partition(),
                record.offset(),
                parsedEvent != null ? parsedEvent.eventId() : null
        );
    }

    private void validateEvent(EmployeeEvent event) {
        if (event.eventId() == null || event.type() == null || event.employeeId() == null || event.occurredAt() == null) {
            throw new IllegalArgumentException("Employee event must contain an ID, type, employee ID, and timestamp");
        }
        if (event.schemaVersion() != EmployeeEvent.CURRENT_SCHEMA_VERSION) {
            throw new IllegalArgumentException("Unsupported employee event schema version: " + event.schemaVersion());
        }
    }

    private boolean claimEvent(UUID eventId) {
        return !processedEventClaimRepository.tryClaim(eventId, Instant.now());
    }

    private void processEvent(EmployeeEvent event) {
        if (event.type() == EmployeeEventType.CREATED) {
            if (event.name() == null || event.department() == null) {
                throw new IllegalArgumentException("Employee creation event must contain name and department");
            }
            employeeRepository.save(EmployeeEntity.from(event));
            meterRegistry.counter("employee.consumer.events", "outcome", "created").increment();
            LOGGER.info("Persisted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
            return;
        }
        if (event.type() == EmployeeEventType.DELETED) {
            employeeRepository.deleteById(event.employeeId());
            meterRegistry.counter("employee.consumer.events", "outcome", "deleted").increment();
            LOGGER.info("Deleted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
            return;
        }
        throw new IllegalArgumentException("Unsupported employee event type: " + event.type());
    }

    private EmployeeEvent parseEvent(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            return objectMapper.readValue(payload, EmployeeEvent.class);
        }
        catch (Exception exception) {
            LOGGER.warn("Unable to parse dead-letter payload", exception);
            return null;
        }
    }

    private String headerValue(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private Integer parseIntegerHeader(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        byte[] value = header.value();
        if (value.length == 4) {
            return java.nio.ByteBuffer.wrap(value).getInt();
        }
        try {
            return Integer.valueOf(new String(value, StandardCharsets.UTF_8));
        }
        catch (NumberFormatException exception) {
            return null;
        }
    }

    private Long parseLongHeader(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        byte[] value = header.value();
        if (value.length == 8) {
            return java.nio.ByteBuffer.wrap(value).getLong();
        }
        try {
            return Long.valueOf(new String(value, StandardCharsets.UTF_8));
        }
        catch (NumberFormatException exception) {
            return null;
        }
    }
}
