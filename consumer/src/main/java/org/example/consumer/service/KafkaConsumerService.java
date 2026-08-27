package org.example.consumer.service;

import org.example.consumer.config.KafkaConsumerProperties;
import org.example.consumer.model.EmployeeEvent;
import org.example.consumer.model.EmployeeEntity;
import org.example.consumer.model.EmployeeEventType;
import org.example.consumer.repository.EmployeeRepository;
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

    public KafkaConsumerService(KafkaConsumerProperties kafkaProperties, EmployeeRepository employeeRepository) {
        this.kafkaProperties = kafkaProperties;
        this.employeeRepository = employeeRepository;
    }

    @KafkaListener(
            topics = "${app.kafka.employee-topic}",
            groupId = "${app.kafka.consumer-group-id}",
            concurrency = "${app.kafka.concurrency}"
    )
    @Transactional
    public void consume(EmployeeEvent event) {
        if (event.type() == null || event.employeeId() == null) {
            throw new IllegalArgumentException("Employee event must contain a type and employee ID");
        }
        if (event.type() == EmployeeEventType.CREATED) {
            if (event.name() == null || event.department() == null) {
                throw new IllegalArgumentException("Employee creation event must contain name and department");
            }
            employeeRepository.save(EmployeeEntity.from(event));
            LOGGER.info("Persisted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
        }
        else if (event.type() == EmployeeEventType.DELETED) {
            employeeRepository.deleteById(event.employeeId());
            LOGGER.info("Deleted employee event: employeeId={}, consumerGroup={}",
                    event.employeeId(), kafkaProperties.consumerGroupId());
        }
    }
}
