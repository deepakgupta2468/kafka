package org.example.consumer;

import java.util.concurrent.TimeUnit;
import org.example.consumer.model.Employee;
import org.example.consumer.repository.EmployeeRepository;
import org.example.consumer.repository.DeadLetterEventRepository;
import org.example.consumer.repository.ProcessedEventRepository;
import org.example.events.EmployeeEvent;
import org.example.events.EmployeeEventType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"employees.v1", "employees.v1.dlt"})
@TestPropertySource(properties = {
        "app.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.kafka.employee-topic=employees.v1",
        "app.kafka.consumer-group-id=employee-service-test",
        "app.kafka.dlt-consumer-group-id=employee-dlt-service-test",
        "app.kafka.concurrency=1",
        "spring.datasource.url=jdbc:h2:mem:employee-consumer;MODE=PostgreSQL;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.jpa.hibernate.ddl-auto=create-drop"
})
class ConsumerApplicationTests {

    @Autowired
    private KafkaTemplate<String, EmployeeEvent> kafkaTemplate;

    @Autowired
    private EmployeeRepository employeeRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private DeadLetterEventRepository deadLetterEventRepository;

    @BeforeEach
    void clearDatabase() {
        deadLetterEventRepository.deleteAll();
        processedEventRepository.deleteAll();
        employeeRepository.deleteAll();
    }

    @Test
    void persistsConsumedEmployee() throws Exception {
        kafkaTemplate.send("employees.v1", "42", employeeCreatedEvent(42L))
                .get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && !employeeRepository.existsById(42L)) {
            Thread.sleep(100);
        }

        assertThat(employeeRepository.existsById(42L)).isTrue();
    }

    @Test
    void deletesEmployeeForConsumedDeletionEvent() throws Exception {
        employeeRepository.save(org.example.consumer.model.EmployeeEntity.from(
                new Employee(99L, "Deepak", "Engineering")
        ));
        kafkaTemplate.send("employees.v1", "99", employeeDeletedEvent(99L))
                .get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && employeeRepository.existsById(99L)) {
            Thread.sleep(100);
        }

        assertThat(employeeRepository.existsById(99L)).isFalse();
    }

    @Test
    void ignoresDuplicateEmployeeEvent() throws Exception {
        java.util.UUID eventId = java.util.UUID.randomUUID();
        EmployeeEvent event = new EmployeeEvent(
                eventId,
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.CREATED,
                7L,
                "Deepak",
                "Engineering",
                java.time.Instant.now()
        );
        kafkaTemplate.send("employees.v1", "7", event).get(10, TimeUnit.SECONDS);
        kafkaTemplate.send("employees.v1", "7", event).get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && processedEventRepository.count() != 1) {
            Thread.sleep(100);
        }

        assertThat(processedEventRepository.count()).isEqualTo(1);
    }

    @Test
    void recordsFailedEventFromDeadLetterTopic() throws Exception {
        EmployeeEvent invalidEvent = new EmployeeEvent(
                java.util.UUID.randomUUID(),
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.CREATED,
                8L,
                null,
                "Engineering",
                java.time.Instant.now()
        );
        kafkaTemplate.send("employees.v1", "8", invalidEvent).get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline && deadLetterEventRepository.count() != 1) {
            Thread.sleep(100);
        }

        assertThat(deadLetterEventRepository.count()).isEqualTo(1);
    }

    private EmployeeEvent employeeCreatedEvent(Long employeeId) {
        return new EmployeeEvent(
                java.util.UUID.randomUUID(),
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.CREATED,
                employeeId,
                "Deepak",
                "Engineering",
                java.time.Instant.now()
        );
    }

    private EmployeeEvent employeeDeletedEvent(Long employeeId) {
        return new EmployeeEvent(
                java.util.UUID.randomUUID(),
                EmployeeEvent.CURRENT_SCHEMA_VERSION,
                EmployeeEventType.DELETED,
                employeeId,
                null,
                null,
                java.time.Instant.now()
        );
    }
}
