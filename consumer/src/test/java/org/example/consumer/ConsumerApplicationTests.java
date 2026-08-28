package org.example.consumer;

import java.util.concurrent.TimeUnit;
import org.example.consumer.model.Employee;
import org.example.consumer.model.EmployeeEvent;
import org.example.consumer.model.EmployeeEventType;
import org.example.consumer.repository.EmployeeRepository;
import org.junit.jupiter.api.Test;
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

    @Test
    void persistsConsumedEmployee() throws Exception {
        kafkaTemplate.send("employees.v1", "42",
                        new EmployeeEvent(EmployeeEventType.CREATED, 42L, "Deepak", "Engineering"))
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
        kafkaTemplate.send("employees.v1", "99", new EmployeeEvent(EmployeeEventType.DELETED, 99L, null, null))
                .get(10, TimeUnit.SECONDS);

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline && employeeRepository.existsById(99L)) {
            Thread.sleep(100);
        }

        assertThat(employeeRepository.existsById(99L)).isFalse();
    }
}
