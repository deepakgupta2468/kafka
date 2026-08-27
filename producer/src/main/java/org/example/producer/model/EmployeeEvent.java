package org.example.producer.model;

public record EmployeeEvent(
        EmployeeEventType type,
        Long employeeId,
        String name,
        String department
) {
    public static EmployeeEvent created(Employee employee) {
        return new EmployeeEvent(EmployeeEventType.CREATED, employee.getId(), employee.getName(), employee.getDepartment());
    }

    public static EmployeeEvent deleted(Long employeeId) {
        return new EmployeeEvent(EmployeeEventType.DELETED, employeeId, null, null);
    }
}
