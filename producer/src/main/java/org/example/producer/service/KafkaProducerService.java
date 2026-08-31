package org.example.producer.service;

import java.time.Instant;
import java.util.UUID;
import org.example.events.EmployeeEvent;
import org.example.events.EmployeeEventType;
import org.example.producer.config.KafkaProducerProperties;
import org.example.producer.model.Employee;
import org.example.producer.outbox.OutboxService;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final OutboxService outboxService;
    private final KafkaProducerProperties kafkaProperties;

    public KafkaProducerService(
            OutboxService outboxService,
            KafkaProducerProperties kafkaProperties
    ) {
        this.outboxService = outboxService;
        this.kafkaProperties = kafkaProperties;
    }

    public UUID sendEmployee(Employee employee) {
        return sendEmployee(employee, UUID.randomUUID());
    }

    public UUID sendEmployee(Employee employee, UUID eventId) {
        EmployeeEvent event = new EmployeeEvent(
                eventId,
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.CREATED,
                employee.getId(),
                employee.getName(),
                employee.getDepartment(),
                Instant.now()
        );
        outboxService.store(event, kafkaProperties.employeeTopic());
        return event.eventId();
    }

    public UUID deleteEmployee(Long employeeId) {
        return deleteEmployee(employeeId, UUID.randomUUID());
    }

    public UUID deleteEmployee(Long employeeId, UUID eventId) {
        EmployeeEvent event = new EmployeeEvent(
                eventId,
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.DELETED,
                employeeId,
                null,
                null,
                Instant.now()
        );
        outboxService.store(event, kafkaProperties.employeeTopic());
        return event.eventId();
    }
}
