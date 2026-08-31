package org.example.producer.outbox;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    @Query(
            value = """
                    SELECT id FROM outbox_events
                    WHERE status = 'PENDING'
                    ORDER BY created_at
                    LIMIT :batchSize
                    FOR UPDATE SKIP LOCKED
                    """,
            nativeQuery = true
    )
    List<UUID> findClaimableEventIds(@Param("batchSize") int batchSize);

    @Modifying
    @Query("UPDATE OutboxEventEntity e SET e.status = org.example.producer.outbox.OutboxStatus.PUBLISHING WHERE e.id IN :ids AND e.status = org.example.producer.outbox.OutboxStatus.PENDING")
    int markPublishing(@Param("ids") List<UUID> ids);
}
