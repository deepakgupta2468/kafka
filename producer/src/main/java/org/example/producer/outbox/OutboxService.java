package org.example.producer.outbox;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.example.events.EmployeeEvent;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxService {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void store(EmployeeEvent event, String topic) {
        try {
            outboxEventRepository.save(new OutboxEventEntity(
                    event.eventId(),
                    event.employeeId().toString(),
                    topic,
                    objectMapper.writeValueAsString(event),
                    Instant.now()
            ));
        }
        catch (JacksonException exception) {
            throw new IllegalStateException("Unable to serialize employee event for the outbox", exception);
        }
    }
}
