package org.example.consumer.repository;

import org.example.consumer.model.DeadLetterEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeadLetterEventRepository extends JpaRepository<DeadLetterEventEntity, Long> {
}
