package org.example.producer.config;

import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        @Min(1) int publishIntervalMs,
        @Min(1) int batchSize,
        @Min(1) int maxRetries
) {
}
