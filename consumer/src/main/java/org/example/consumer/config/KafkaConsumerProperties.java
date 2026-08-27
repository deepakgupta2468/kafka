package org.example.consumer.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaConsumerProperties(
        @NotBlank String bootstrapServers,
        @NotBlank String employeeTopic,
        @NotBlank String consumerGroupId,
        @Min(1) int topicPartitions,
        @Min(1) short topicReplicationFactor,
        @Min(1) int concurrency
) {
    public String employeeDltTopic() {
        return employeeTopic + ".dlt";
    }
}
