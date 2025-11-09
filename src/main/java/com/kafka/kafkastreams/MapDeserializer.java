package com.kafka.kafkastreams;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MapDeserializer implements Deserializer<Map<String, Long>> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Map<String, Long> deserialize(String topic, byte[] data) {
        try {
            if (data == null) return null;
            return mapper.readValue(data, new TypeReference<Map<String, Long>>() {});
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}