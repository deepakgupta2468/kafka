package org.example.producer.service;

import java.time.Instant;
import java.util.UUID;
import org.example.producer.config.KafkaProducerProperties;
import org.example.producer.model.Employee;
import org.example.producer.outbox.OutboxService;
import org.example.events.EmployeeEvent;
import org.example.events.EmployeeEventType;
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

    public void sendEmployee(Employee employee) {
        EmployeeEvent event = new EmployeeEvent(
                UUID.randomUUID(),
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.CREATED,
                employee.getId(),
                employee.getName(),
                employee.getDepartment(),
                Instant.now()
        );
        outboxService.store(event, kafkaProperties.employeeTopic());
    }

    public void deleteEmployee(Long employeeId) {
        EmployeeEvent event = new EmployeeEvent(
                UUID.randomUUID(),
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.DELETED,
                employeeId,
                null,
                null,
                Instant.now()
        );
        outboxService.store(event, kafkaProperties.employeeTopic());
    }
}
