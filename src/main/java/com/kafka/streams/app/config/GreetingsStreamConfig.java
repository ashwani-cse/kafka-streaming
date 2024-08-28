package com.kafka.streams.app.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.streams.app.topology.GreetingStreamsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.HashMap;
import java.util.Map;
@Slf4j
@Configuration
public class GreetingsStreamConfig {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = kafkaProperties.buildStreamsProperties(null);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
        return new KafkaStreamsConfiguration(props);
    }

    private ConsumerRecordRecoverer recoverer(){
        return ((consumerRecord, e) -> {
            log.error("Exception is: {}, failed Record : {}", consumerRecord, e.getMessage(),e);
        });
    }

/*
    @Bean
    public DeadLetterPublishingRecoverer recovere1r() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate(),
                (record, ex) -> new TopicPartition("recovererDLQ", -1));
    }
*/

    @Bean
    public NewTopic greetingsTopic() {
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic greetingsOutputTopic() {
        return TopicBuilder.name(GreetingStreamsTopology.GREETINGS_OUTPUT)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper objectMapper = new ObjectMapper()
                // This method registers a module with the ObjectMapper. Specifically, JavaTimeModule is a module that adds support for Java 8 Date and Time API (like LocalDate, LocalDateTime, etc.).
                // Without registering this module, the ObjectMapper would not know how to serialize and deserialize the new date and time types introduced in Java 8.
                .registerModule(new JavaTimeModule())

                // This line configures the ObjectMapper to change its default behavior regarding how dates are serialized.
                // SerializationFeature.WRITE_DATES_AS_TIMESTAMPS is a feature that, when set to true, will serialize dates as numeric timestamps (milliseconds since epoch).
                // By setting this feature to false, dates will be serialized as strings in ISO-8601 format (e.g., "2023-10-05T14:48:00.000Z").
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return objectMapper;
    }
}
