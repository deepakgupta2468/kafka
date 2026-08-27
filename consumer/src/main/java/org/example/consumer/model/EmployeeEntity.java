package org.example.consumer.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "employees")
public class EmployeeEntity {

    @Id
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String department;

    protected EmployeeEntity() {
    }

    private EmployeeEntity(Long id, String name, String department) {
        this.id = id;
        this.name = name;
        this.department = department;
    }

    public static EmployeeEntity from(Employee employee) {
        return new EmployeeEntity(employee.getId(), employee.getName(), employee.getDepartment());
    }

    public static EmployeeEntity from(EmployeeEvent event) {
        return new EmployeeEntity(event.employeeId(), event.name(), event.department());
    }
}
