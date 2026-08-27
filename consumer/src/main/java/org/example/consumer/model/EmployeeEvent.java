package org.example.consumer.model;

public record EmployeeEvent(
        EmployeeEventType type,
        Long employeeId,
        String name,
        String department
) {
}
