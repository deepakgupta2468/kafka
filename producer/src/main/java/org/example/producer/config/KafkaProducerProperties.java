package org.example.producer.config;

import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaProducerProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String employeeTopic
) {
}
