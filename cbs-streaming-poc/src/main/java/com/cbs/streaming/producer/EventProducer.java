package com.cbs.streaming.producer;

import com.cbs.streaming.constants.EventsConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Ashwani Kumar
 * Created on 06/09/24.
 */
@Slf4j
public class EventProducer {
    static ObjectMapper objectMapper = new ObjectMapper();
    static String topicName = EventsConfig.Account.POSTING_TOPIC;

    public static void main(String[] args) {
        // String jsonArrayFileName = "account_events.json";
        String jsonArrayFileName = "account_events_samples.json";
        InputStream inputStream = EventProducer.class.getClassLoader()
                .getResourceAsStream(jsonArrayFileName);


        try {
            JsonNode jsonNode = objectMapper.readTree(inputStream);
            publishMessageSync(jsonNode.get(0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void publishMessageSync(JsonNode jsonNode) {
        {
            try {
                String key = jsonNode.get("event_id").asText();
                String jsonEvent = objectMapper.writeValueAsString(jsonNode);
                log.info("key : {}, jsonEvent : {}", key, jsonEvent);
                ProducerUtil.publishMessageSync(topicName, key, jsonEvent);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
