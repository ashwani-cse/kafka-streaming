package com.cbs.streaming.topology;

import com.cbs.streaming.constants.EventsConfig;
import com.cbs.streaming.util.AccountDeduplicationProcessor;
import com.cbs.streaming.util.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import static com.cbs.streaming.constants.EventsConfig.Account.*;

@Component
@Slf4j
public class AccountEventTopology {

    private StreamsBuilder streamsBuilder;

    public AccountEventTopology(StreamsBuilder streamsBuilder) {
        this.streamsBuilder = streamsBuilder;
    }

    @PostConstruct
    public void init() {
        Topology topology = buildAccountTopology();
        log.info("Starting Kafka Streams with topology: {}", topology.describe());
    }

    public Topology buildAccountTopology() {

        // Step 1: Set up the RocksDB state store for deduplication
        StoreBuilder<KeyValueStore<String, JsonNode>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(ACCOUNT_POSTING_STORE),
                        Serdes.String(),
                        new JsonSerde<>(JsonNode.class)
                );
        streamsBuilder.addStateStore(storeBuilder);

        // Step 2: Stream processing for account creation
        Predicate<String, JsonNode> accountCreatedPredicate = (key, value) -> value.has(ACCOUNT_CREATED_EVENT);
        accountEventHandling(streamsBuilder, accountCreatedPredicate, CREATE_TOPIC);

        // Step 3: Stream processing for account updates
        Predicate<String, JsonNode> accountUpdatePredicate = (key, value) -> value.has(ACCOUNT_UPDATED_EVENT) || value.has(ACCOUNT_UPDATED_CREATED_EVENT);
        accountEventHandling(streamsBuilder, accountUpdatePredicate, UPDATE_TOPIC);

        // Step 4: Stream processing for account balance updates
        Predicate<String, JsonNode> balanceEventPredicate = (key, value) -> value.has(ACCOUNT_BALANCE_EVENT);
        accountEventHandling(streamsBuilder, balanceEventPredicate, BALANCE_TOPIC);

        return streamsBuilder.build();
    }

    private void accountEventHandling(StreamsBuilder streamsBuilder, Predicate<String, JsonNode> eventType, String downStreamTopic) {
        streamsBuilder
                .stream(POSTING_TOPIC,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .map((key, value) -> new KeyValue<>(key, JsonUtil.getNode(value)))
                .filter(eventType)
                .peek((key, value) -> log.info("Before Deduplication Processing event: key: {}, value: {}", key, value))
                .process(() -> new AccountDeduplicationProcessor(downStreamTopic),
                        ACCOUNT_POSTING_STORE) //
                .peek((key, value) -> log.info("After Deduplication Processing event: key: {}, value: {}", key, value))
                .to(downStreamTopic,
                        Produced.with(Serdes.String(), new JsonSerde<>(JsonNode.class)));

        // Define DLQ processing for dynamically generated DLQ topics
        streamsBuilder
                .stream(EventsConfig.DLQ_PREFIX + downStreamTopic,
                        Consumed.with(Serdes.String(), new JsonSerde<>(JsonNode.class)))
                .peek((key, value) -> log.error("DLQ Event: Key={}, Value={}", key, value));

    }

/*    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        accountTopology(streamsBuilder);
    }*/
}
