package com.cbs.streaming.util;

import com.cbs.streaming.constants.EventsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
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
import static com.cbs.streaming.constants.EventsConfig.Account.POSTING_TOPIC;

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
    private String dlqTopic;

    public AccountDeduplicationProcessor(String dlqTopicIdentifier) {
        this.dlqTopic = EventsConfig.DLQ_PREFIX + dlqTopicIdentifier;
    }

    @Override
    public void init(ProcessorContext<String, JsonNode> context) {
        this.context = context;
        this.store = context.getStateStore(ACCOUNT_POSTING_STORE); // Here, must refer to the same store name as defined in the topology. If the store name is different, it will throw an exception.

        //Java 7 way of implementing punctuate
        Punctuator cleanupPunctuator = new Punctuator() { // type 1
            @Override
            public void punctuate(long l) {
                enforceTTL(l);
            }
        };

        //Java 8 way of implementing punctuate
        cleanupPunctuator = (long timestamp) -> this.enforceTTL(timestamp); // type 2
        cleanupPunctuator = this::enforceTTL; // type 3
// schedule a punctuate call to the processor every minute to clean up the store.
        this.punctuator = context.schedule(
                Duration.ofMinutes(1),
                PunctuationType.WALL_CLOCK_TIME, cleanupPunctuator); // The periodic task will run every minute to ensure that records older than a certain TTL (time-to-live) are evicted from the store.

        Punctuator commitPunctuator = (long ts) -> context.commit();

        // schedule a punctuate call to the processor every 20 seconds to commit the current processing progress.
        this.context.schedule(
                Duration.ofSeconds(20),
                PunctuationType.WALL_CLOCK_TIME, // WALL_CLOCK_TIME schedules the punctuate call based on the system clock.
                commitPunctuator);
        // (ts) -> context.commit()); // add commit punctuator directly also like this
    }

    private void enforceTTL(long timestamp) {
        log.info("Enforcing TTL at {}", Instant.ofEpochMilli(timestamp));
        try (KeyValueIterator<String, JsonNode> iterator = store.all()) {
            Spliterator<KeyValue<String, JsonNode>> keyValueSpliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
            keyValueSpliterator.forEachRemaining(keyValue -> {
                Instant streamStartupTimestamp = Instant.parse(keyValue.value.get("timestamp").asText());
                Duration duration = Duration.between(streamStartupTimestamp, Instant.now());
                if (duration.toMinutes() > 30) {
                    log.info("Record with key: {} is evicted!", keyValue.key);
                    store.delete(keyValue.key);
                }
            });
        } catch (Exception e) {
            log.error("Error during TTL enforcement", e);
        }
    }


    /**
     * Process the record. Note that record metadata is undefined in cases such as a forward call from a punctuator.
     *
     * @param record the record to process
     */
    @Override
    public void process(Record<String, JsonNode> record) {
        try {
            String key = record.value().get("event_id").asText(); // Key to store the record
            JsonNode storedRecord = store.get(key);
            int retryCount = storedRecord != null && storedRecord.has("retry_count")
                    ? storedRecord.get("retry_count").asInt()
                    : 0;
            if (storedRecord == null || retryCount < 3) {
                log.info("New record found: {}", record.value());
                amendRecordWithCorrelationId(record); // Amend the record with correlation id
                Record<String, JsonNode> newRecord =
                        new Record<>(record.key(), record.value(), record.timestamp());
                if (true)
                    throw new RuntimeException();
                store.put(key, record.value()); // Store the record to deduplicate
                context.forward(newRecord); // Forward the record to the downstream processors

                // we can also commit the current processing progress by calling context.commit() in the process method.
                // This is useful when the processor is stateless and does not need to maintain any state.
                // context.commit();
            } else {
                // Duplicate record
                log.info("1. Duplicate record found: {}", record.value());
                System.out.println("2. Duplicate record found: " + record.value());
            }
        } catch (Exception e) {
            log.error("Error processing record: {}, sending to DLQ. Exception: {}", record.value(), e.getMessage(), e);
            handleRetry(record, this.dlqTopic, e);
        }
    }

    private void amendRecordWithCorrelationId(Record<String, JsonNode> record) {
        if (record.value() instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) record.value();
            Optional<RecordMetadata> recordMetadataOptional = context.recordMetadata();
            if (recordMetadataOptional.isPresent()) {
                RecordMetadata recordMetadata = recordMetadataOptional.get();
                String correlationId = recordMetadata.partition() + "-" + recordMetadata.offset() + "-" + record.key();
                objectNode.put("correlation_id", correlationId);
            }
        } else {
            log.warn("Record value is not an ObjectNode: {}", record.value());
        }
    }

    private void handleRetry(Record<String, JsonNode> record, String dlqTopic, Exception e) {
        int MAX_RETRY_COUNT = 3;
        int retryCount = record.value().has("retry_count") ? record.value().get("retry_count").asInt() : 0;
        retryCount++;

        ObjectNode recordWithRetryCount = ((ObjectNode) record.value()).put("retry_count", retryCount);
        store.put(record.key(), recordWithRetryCount); // Store the record with updated retry count
        
        if (true) {
            log.error("### - Max retry count reached. Sending to DLQ: {}", record.value());
          //  context.forward(new Record<>(record.key(), recordWithRetryCount, record.timestamp()), dlqTopic); // not working like this
        } else {
            log.info("$$$ - Retrying record: {}", record.value());
          //  context.forward(new Record<>(record.key(), recordWithRetryCount, record.timestamp()), POSTING_TOPIC);
        }
    }

    @Override
    public void close() {
        // close any resources managed by this processor
        // Note: Do not close any StateStores as these are managed by the library
        if (punctuator != null) {
            punctuator.cancel();
        }
    }
}
