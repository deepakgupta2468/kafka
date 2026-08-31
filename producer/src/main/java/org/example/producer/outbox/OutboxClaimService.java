package org.example.producer.outbox;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxClaimService {

    private final OutboxEventRepository outboxEventRepository;

    public OutboxClaimService(OutboxEventRepository outboxEventRepository) {
        this.outboxEventRepository = outboxEventRepository;
    }

    @Transactional
    public List<OutboxEventEntity> claimPendingEvents(int batchSize) {
        List<UUID> claimableIds = outboxEventRepository.findClaimableEventIds(batchSize);
        if (claimableIds.isEmpty()) {
            return List.of();
        }
        int claimed = outboxEventRepository.markPublishing(claimableIds);
        if (claimed == 0) {
            return List.of();
        }
        return outboxEventRepository.findAllById(claimableIds).stream()
                .filter(event -> event.getStatus() == OutboxStatus.PUBLISHING)
                .toList();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPublished(UUID eventId) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.markPublished(Instant.now());
            outboxEventRepository.save(event);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markRetryableFailure(UUID eventId, String error, int maxRetries) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            if (event.getRetryCount() + 1 >= maxRetries) {
                event.markFailed(error);
            }
            else {
                event.markPendingAfterFailure(error);
            }
            outboxEventRepository.save(event);
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markPermanentFailure(UUID eventId, String error) {
        outboxEventRepository.findById(eventId).ifPresent(event -> {
            event.markFailed(error);
            outboxEventRepository.save(event);
        });
    }
}
