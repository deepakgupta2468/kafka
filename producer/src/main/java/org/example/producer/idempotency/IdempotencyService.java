package org.example.producer.idempotency;

import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class IdempotencyService {

    private final IdempotencyKeyRepository idempotencyKeyRepository;

    public IdempotencyService(IdempotencyKeyRepository idempotencyKeyRepository) {
        this.idempotencyKeyRepository = idempotencyKeyRepository;
    }

    @Transactional
    public IdempotentResult execute(String idempotencyKey, Consumer<UUID> action) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            UUID eventId = UUID.randomUUID();
            action.accept(eventId);
            return new IdempotentResult(eventId, false);
        }

        String key = idempotencyKey.trim();
        return idempotencyKeyRepository.findById(key)
                .map(existing -> new IdempotentResult(existing.getEventId(), true))
                .orElseGet(() -> reserveAndExecute(key, action));
    }

    private IdempotentResult reserveAndExecute(String key, Consumer<UUID> action) {
        UUID eventId = UUID.randomUUID();
        try {
            idempotencyKeyRepository.saveAndFlush(new IdempotencyKeyEntity(key, eventId, Instant.now()));
        }
        catch (DataIntegrityViolationException exception) {
            return idempotencyKeyRepository.findById(key)
                    .map(existing -> new IdempotentResult(existing.getEventId(), true))
                    .orElseThrow(() -> exception);
        }
        action.accept(eventId);
        return new IdempotentResult(eventId, false);
    }
}
