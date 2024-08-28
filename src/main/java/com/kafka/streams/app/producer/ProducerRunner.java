package com.kafka.streams.app.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kafka.streams.app.domain.Greeting;
import com.kafka.streams.app.topology.GreetingStreamsTopology;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * @author Ashwani Kumar
 * Created on 27/07/24.
 */
public class ProducerRunner {
    private static final Logger logger = LoggerFactory.getLogger(ProducerRunner.class.getName());

    private static ProducerUtil producerUtil = new ProducerUtil();

    private static ObjectMapper objectMapper = new ObjectMapper()
            // This method registers a module with the ObjectMapper. Specifically, JavaTimeModule is a module that adds support for Java 8 Date and Time API (like LocalDate, LocalDateTime, etc.).
            // Without registering this module, the ObjectMapper would not know how to serialize and deserialize the new date and time types introduced in Java 8.
            .registerModule(new JavaTimeModule())

            // This line configures the ObjectMapper to change its default behavior regarding how dates are serialized.
            // SerializationFeature.WRITE_DATES_AS_TIMESTAMPS is a feature that, when set to true, will serialize dates as numeric timestamps (milliseconds since epoch).
            // By setting this feature to false, dates will be serialized as strings in ISO-8601 format (e.g., "2023-10-05T14:48:00.000Z").
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

    public static void main(String[] args) {

        Runnable producer1 = produceEnglishGreetingMessage();
        Runnable producer2 = produceSpanishGreetingMessage();

        Thread t1 = new Thread(producer1, "English Producer");
        Thread t2 = new Thread(producer2, "Spanish Producer");
        t1.start();
       // t2.start();
        try {
            t1.join();
          //  t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        producerUtil.closeProducer();
    }

    private static Runnable produceSpanishGreetingMessage() {
        return () -> {
            String key = "P2_message";
            key = null;
            for (int i = 0; i < 20; i++) {
                String message = "Hola " + i + " from " + Thread.currentThread().getName();
              //  produceMessage(GreetingStreamsTopology.GREETINGS_SPANIISH, key, message);
            }
        };
    }

    private static Runnable produceEnglishGreetingMessage() {
        return () -> {
            //TopicCreater.create(topic, 3, 1);
            // delay(30000);
            String key = "P1_message";
            key = null;
            for (int i = 0; i < 20; i++) {
                String message = "Hi " + i + " from " + Thread.currentThread().getName();
                produceMessage(GreetingStreamsTopology.GREETINGS, key, message);
            }
        };
    }

    private static void produceMessage(String topic, String key, String message) {
        Greeting greeting = new Greeting(message, LocalDateTime.now());
        try {
            String jsonMessage = objectMapper.writeValueAsString(greeting);
            logger.info("Greeting Json Message: " + jsonMessage);
            RecordMetadata recordMetadata = producerUtil.publishMessageSync(topic, key, jsonMessage);
            logger.info("Greeting Record Metadata: " + recordMetadata.partition());
            delay(6000);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static void delay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}