package org.example.producer.controller;

import jakarta.validation.Valid;
import org.example.producer.model.Employee;
import org.example.producer.service.KafkaProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    private final KafkaProducerService producerService;

    public KafkaController(KafkaProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/employees")
    public ResponseEntity<Void> sendEmployee(@Valid @RequestBody Employee employee) {
        for (int i = 1001; i < 1010; i++) {
            employee.setId((long)i);
            producerService.sendEmployee(employee);
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }
}
