package org.example.producer.controller;

import jakarta.validation.Valid;
import org.example.producer.model.Employee;
import org.example.producer.service.KafkaProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
    public ResponseEntity<EmployeeEventResponse> sendEmployee(@Valid @RequestBody Employee employee) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new EmployeeEventResponse(producerService.sendEmployee(employee)));
    }

    @DeleteMapping("/employees/{employeeId}")
    public ResponseEntity<EmployeeEventResponse> deleteEmployee(@PathVariable Long employeeId) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(new EmployeeEventResponse(producerService.deleteEmployee(employeeId)));
    }
}
