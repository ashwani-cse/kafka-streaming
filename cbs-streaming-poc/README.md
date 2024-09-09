# Kafka Streams Account Deduplication Processor

## Overview

This project demonstrates how to use Kafka Streams to process and deduplicate account-related events. The primary goal of the `AccountDeduplicationProcessor` is to ensure that each event is processed only once, based on its unique identifier (`event_id`). This is achieved by leveraging a RocksDB state store for deduplication and a changelog topic to maintain the state store's consistency.

## Components

### 1. **AccountDeduplicationProcessor**

- **Purpose:** Implements the `Processor` interface to perform deduplication of account-related events.
- **Key Operations:**
    - **State Store:** Stores records to prevent duplicates.
    - **Punctuator:** Periodically enforces TTL (Time-To-Live) and commits offsets to ensure data consistency and fault tolerance.
    - **Processing Logic:** Checks for the presence of a record with the same key and processes new records while identifying duplicates.

#### Key Methods

- **`init(ProcessorContext<String, JsonNode> context)`**
    - Initializes the processor with the state store and sets up punctuators for TTL enforcement and offset commits.

- **`process(Record<String, JsonNode> record)`**
    - Processes incoming records, checking for duplicates and amending records with a correlation ID.

- **`enforceTTL(long l)`**
    - Enforces TTL to evict old records from the state store, ensuring that only recent records are retained.

- **`amendRecordWithCorrelationId(Record<String, JsonNode> record)`**
    - Adds a correlation ID to each record for tracking and debugging purposes.

### 2. **AccountEventTopology**

- **Purpose:** Configures the Kafka Streams topology, including stream processing and state store setup.
- **Key Operations:**
    - **State Store Setup:** Defines a persistent key-value store using RocksDB.
    - **Stream Processing:** Processes events from input topics, applies filtering, and routes processed events to output topics.

#### Key Methods

- **`accountTopology(StreamsBuilder streamsBuilder)`**
    - Builds the Kafka Streams topology with state store configuration and event handling.

- **`accountEventHandling(StreamsBuilder streamsBuilder, Predicate<String, JsonNode> eventType, String downStreamTopic)`**
    - Defines how different types of events are processed and forwarded to downstream topics.

## State Store and Changelog

### State Store

The state store is a persistent local store (backed by RocksDB) used to maintain state information for Kafka Streams processing. It helps in performing operations such as joins, aggregations, or deduplication.

### Changelog Topic

The changelog topic records every change made to the state store. This includes updates, deletions, and new entries. It ensures that the state store can be rebuilt in case of application restarts or crashes.

### Tombstones

**Tombstones** are special records in the changelog topic with a key but a `null` value. They indicate that a record with the specified key has been deleted from the state store. Here’s how they work:

- **Purpose:** Tombstones are used to signal deletions. They help maintain consistency by ensuring that deletions in the state store are propagated and applied across all replicas.

- **Behavior:** When a tombstone is written to the changelog topic, it tells all consumers (including replicas) that the key should be removed from the state store. This ensures that the state store is accurately synchronized and reflects the current state of the data.

**Example Scenario:**

1. **Initial Insert:** A record with key `123456` is inserted into the state store and recorded in the changelog topic.
2. **Deletion:** Later, the record is removed from the state store (e.g., due to TTL enforcement). A tombstone with a `null` value is written to the changelog to indicate this deletion.
3. **Re-insertion:** If a new record with the same key `123456` is processed, it overwrites the tombstone in the state store and is recorded again in the changelog topic.

**Why `null` Values Appear:**

- **State Store Cleanup:** Tombstones indicate that records have been deleted from the state store.
- **Expired Records:** Records older than the TTL are removed and represented by tombstones.
- **Reprocessing:** Tombstones may appear before new records if a record with the same key is re-inserted.

## Running the Application

### Prerequisites

- Kafka and Zookeeper should be up and running.
- Kafka Streams dependencies must be included in your project.

### How to Run

1. **Build the Project:** Compile and package the application using your preferred build tool.
2. **Start Kafka Streams Application:** Run the application, which will start processing events as per the defined topology.
3. **Monitor Logs:** Check the logs for detailed information about record processing, deduplication, and TTL enforcement.

## Conclusion

This project demonstrates a practical implementation of Kafka Streams for deduplication and event processing. By understanding the role of the state store, changelog, and tombstones, you can effectively manage and process streaming data in a fault-tolerant manner.

