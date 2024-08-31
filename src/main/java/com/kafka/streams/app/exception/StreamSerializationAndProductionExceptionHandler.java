package com.kafka.streams.app.exception;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Ashwani Kumar
 * Created on 18/08/24.
 * This class is used to handle the exception in the serialization of the Kafka Record or if rebalancing is happening or cluster is down
 */
public class StreamSerializationAndProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(StreamSerializationAndProductionExceptionHandler.class);

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        log.error("Exception is : {} and the Kafka Record is : {} ", exception.getMessage(), record, exception);
        return ProductionExceptionHandlerResponse.CONTINUE; //
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}