package com.cbs.streaming.util;

/**
 * @author Ashwani Kumar
 * Created on 07/09/24.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Parses a JSON string into a JsonNode.
     */
    public static JsonNode getNode(String jsonString) {
        try {
            return objectMapper.readTree(jsonString);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse JSON string: " + jsonString, e);
        }
    }

    /**
     * Serializes an object into a JSON string.
     */
    public static String toJsonString(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object to JSON: " + object, e);
        }
    }

    /**
     * Parses a JsonNode into a specific Java object.
     */
    public static <T> T fromJsonNode(JsonNode jsonNode, Class<T> clazz) {
        try {
            return objectMapper.treeToValue(jsonNode, clazz);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert JsonNode to object: " + jsonNode, e);
        }
    }

    /**
     * Converts a Java object to a JsonNode.
     */
    public static JsonNode toJsonNode(Object object) {
        try {
            return objectMapper.valueToTree(object);
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert object to JsonNode: " + object, e);
        }
    }
}
