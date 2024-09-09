package com.cbs.streaming.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.api.RecordMetadata;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;

import static com.cbs.streaming.constants.EventsConfig.Account.ACCOUNT_POSTING_STORE;

/**
 * @author Ashwani Kumar
 * Created on 07/09/24.
 */
@Slf4j
public class AccountDeduplicationProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context; // It provides access to metadata of the record being processed, such as its source topic-partition, its offset, and the timestamp of the record.
    private KeyValueStore<String, JsonNode> store; // Store for deduplication
    private Cancellable punctuator; // Used to schedule a periodic punctuate call to the processor;
    // Punctuate is used to perform periodic actions such as committing offsets or forwarding records to downstream processors.
    // for example, to forward a record to a downstream processor every 10 seconds, you would schedule a punctuate call every 10 seconds.
    // Without punctuate, the processor would only receive records when they arrive, and would not be able to forward records to downstream processors at regular intervals.

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        this.context = context;
        this.store = context.getStateStore(ACCOUNT_POSTING_STORE);
        this.punctuator = context.schedule(
                Duration.ofMinutes(1),
                PunctuationType.WALL_CLOCK_TIME, this::enforceTTL); // The periodic task will run every minute to ensure that records older than a certain TTL (time-to-live) are evicted from the store.
        this.context.schedule(
                Duration.ofSeconds(20),
                PunctuationType.WALL_CLOCK_TIME, // WALL_CLOCK_TIME schedules the punctuate call based on the system clock.
                (ts) -> context.commit()); // Commit the offsets every 20 seconds; This prevents data loss by regularly committing offsets, so the stream can resume from the last committed offset in case of a crash.
    }

    private void enforceTTL(long l) {
        log.info("Enforcing TTL");
        KeyValueIterator<String, JsonNode> jsonNodeKeyValueIterator = store.all();
        Spliterator<KeyValue<String, JsonNode>> keyValueSpliterator = Spliterators.spliteratorUnknownSize(jsonNodeKeyValueIterator, 0);
        keyValueSpliterator.forEachRemaining(keyValue -> {
            Instant streamStartupTimestamp = Instant.parse(keyValue.value.get("timestamp").asText());
            Duration duration = Duration.between(streamStartupTimestamp, Instant.now());
            if (duration.toMinutes() > 30) {
                log.info("Record with key: {} is evicted!", keyValue.key);
                store.delete(keyValue.key);
            }
        });
    }


    /**
     * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
     *
     * @param record the record to process
     */
    @Override
    public void process(Record<String, JsonNode> record) {
        String key = record.value().get("event_id").asText(); // Key to store the record
        if (store.get(key) == null) {
            log.info("New record found: {}", record.value());
            amendRecordWithCorrelationId(record); // Amend the record with correlation id
            Record<String, JsonNode> newRecord =
                    new Record<>(record.key(), record.value(), record.timestamp());
            store.put(key, record.value()); // Store the record to deduplicate
            context.forward(newRecord); // Forward the record to the downstream processors
        } else {
            // Duplicate record
            log.info("1. Duplicate record found: {}", record.value());
            System.out.println("2. Duplicate record found: "+record.value());
        }

    }

    private void amendRecordWithCorrelationId(Record<String, JsonNode> record) {
        ObjectNode objectNode = (ObjectNode) record.value();
        Optional<RecordMetadata> recordMetadataOptional = this.context.recordMetadata();
        if (recordMetadataOptional.isPresent()) {
            RecordMetadata recordMetadata = recordMetadataOptional.get();
            String correlationId = new StringBuilder()
                    .append(recordMetadata.partition())
                    .append(recordMetadata.offset())
                    .append(record.key())
                    .toString();
            objectNode.put("correlation_id", correlationId);
        }

    }
}
