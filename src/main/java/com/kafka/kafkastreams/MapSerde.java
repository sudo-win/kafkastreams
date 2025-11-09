package com.kafka.kafkastreams;

import org.apache.kafka.common.serialization.Serdes;
import java.util.Map;

public class MapSerde extends Serdes.WrapperSerde<Map<String, Long>> {
    public MapSerde() {
        super(new MapSerializer(), new MapDeserializer());
    }
}