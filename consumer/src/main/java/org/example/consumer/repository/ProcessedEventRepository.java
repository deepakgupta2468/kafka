package org.example.consumer.repository;

import java.util.UUID;
import org.example.consumer.model.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, UUID> {
}
