package com.cbs.streaming.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

import static com.cbs.streaming.constants.EventsConfig.Account.*;
import static com.cbs.streaming.constants.EventsConfig.DLQ_PREFIX;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic accountTopic() {
        return TopicBuilder.name(POSTING_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic transactionTopic() {
        return TopicBuilder.name(CREATE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic updateTopic() {
        return TopicBuilder.name(UPDATE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic balanceTopic() {
        return TopicBuilder.name(BALANCE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic createAccountDLQEventTopic() {
        return TopicBuilder.name(DLQ_PREFIX + CREATE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic updateAccountDLQEventTopic() {
        return TopicBuilder.name(DLQ_PREFIX + UPDATE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic accountBalanceDLQEventTopic() {
        return TopicBuilder.name(DLQ_PREFIX + BALANCE_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}