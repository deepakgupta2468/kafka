package org.example.producer.service;

import java.util.concurrent.CompletableFuture;
import org.example.producer.config.KafkaProducerProperties;
import org.example.producer.model.Employee;
import org.example.producer.model.EmployeeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaTemplate<String, EmployeeEvent> kafkaTemplate;
    private final KafkaProducerProperties kafkaProperties;

    public KafkaProducerService(
            KafkaTemplate<String, EmployeeEvent> kafkaTemplate,
            KafkaProducerProperties kafkaProperties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    public CompletableFuture<SendResult<String, EmployeeEvent>> sendEmployee(Employee employee) {
        EmployeeEvent event = EmployeeEvent.created(employee);
        CompletableFuture<SendResult<String, EmployeeEvent>> result = kafkaTemplate.send(
                kafkaProperties.employeeTopic(), employee.getId().toString(), event
        );
        result.whenComplete((sendResult, exception) -> {
            if (exception == null) {
                LOGGER.info("Published employee event: employeeId={}, topic={}, partition={}, offset={}",
                        employee.getId(), sendResult.getRecordMetadata().topic(),
                        sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
            }
            else {
                LOGGER.error("Failed to publish employee event: employeeId={}", employee.getId(), exception);
            }
        });
        return result;
    }

    public CompletableFuture<SendResult<String, EmployeeEvent>> deleteEmployee(Long employeeId) {
        EmployeeEvent event = EmployeeEvent.deleted(employeeId);
        CompletableFuture<SendResult<String, EmployeeEvent>> result = kafkaTemplate.send(
                kafkaProperties.employeeTopic(), employeeId.toString(), event
        );
        result.whenComplete((sendResult, exception) -> {
            if (exception == null) {
                LOGGER.info("Published employee deletion event: employeeId={}, topic={}, partition={}, offset={}",
                        employeeId, sendResult.getRecordMetadata().topic(),
                        sendResult.getRecordMetadata().partition(), sendResult.getRecordMetadata().offset());
            }
            else {
                LOGGER.error("Failed to publish employee deletion event: employeeId={}", employeeId, exception);
            }
        });
        return result;
    }
}
