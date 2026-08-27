package org.example.consumer.service;

import org.example.consumer.config.KafkaConsumerProperties;
import org.example.consumer.model.Employee;
import org.example.consumer.model.EmployeeEntity;
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
    public void consume(Employee employee) {
        employeeRepository.save(EmployeeEntity.from(employee));
        LOGGER.info("Persisted employee event: employeeId={}, name={}, department={}, consumerGroup={}",
                employee.getId(), employee.getName(), employee.getDepartment(), kafkaProperties.consumerGroupId());
    }
}
