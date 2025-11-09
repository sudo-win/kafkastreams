package com.kafka.kafkastreams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MapSerializer implements Serializer<Map<String, Long>> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, Map<String, Long> data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}