package org.example.producer.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.events.EmployeeEvent;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@Configuration
public class KafkaProducerConfig {

    private final KafkaProducerProperties kafkaProperties;
    private final KafkaProperties springKafkaProperties;

    public KafkaProducerConfig(KafkaProducerProperties kafkaProperties, KafkaProperties springKafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.springKafkaProperties = springKafkaProperties;
    }

    @Bean
    public ProducerFactory<String, EmployeeEvent> producerFactory() {
        Map<String, Object> props = new HashMap<>(springKafkaProperties.buildProducerProperties());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, EmployeeEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
