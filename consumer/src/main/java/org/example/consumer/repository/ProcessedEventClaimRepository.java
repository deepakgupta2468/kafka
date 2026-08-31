package org.example.consumer.repository;

import java.time.Instant;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class ProcessedEventClaimRepository {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventClaimRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean tryClaim(UUID eventId, Instant processedAt) {
        try {
            return jdbcTemplate.update(
                    "INSERT INTO processed_events (event_id, processed_at) VALUES (?, ?)",
                    eventId,
                    processedAt
            ) == 1;
        }
        catch (DataIntegrityViolationException exception) {
            return false;
        }
    }
}
