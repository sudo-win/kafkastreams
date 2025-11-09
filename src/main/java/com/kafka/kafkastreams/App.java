package com.kafka.kafkastreams;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {

        

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "errorlog-topic-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        String topic_name = "errorlog-topic";

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Building stream");

        KStream<String, String> stream = builder.stream(topic_name);

        System.out.println("Stream started");

        KStream<String, String> serviceErrors = stream.map((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                String errorType = node.get("error_type").asText();
                return new KeyValue<>("all", errorType);
            } catch (Exception e) {
                return new KeyValue<>("all", "KAFKA_PARSE_ERROR");
            }
        });

        Duration windowSize = Duration.ofMinutes(5);
        Duration advance = Duration.ofMinutes(1);

        TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advance);

        KTable<Windowed<String>, Map<String, Long>> errorCounts = serviceErrors
                .groupByKey()
                .windowedBy(timeWindow)
                .aggregate(
                        LinkedHashMap::new, // initializer
                        (key, value, agg) -> {
                            agg.put(value, agg.getOrDefault(value, 0L) + 1);

                            Map.Entry<String, Long> topEntry = agg.entrySet().stream()
                                    .max(Map.Entry.comparingByValue())
                                    .get();

                            Map<String, Long> topMap = new LinkedHashMap<>();
                            topMap.put(topEntry.getKey(), topEntry.getValue());

                            return topMap;
                        },
                        Materialized.<String, Map<String, Long>, WindowStore<Bytes, byte[]>>as("error-counts-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new MapSerde()));



    errorCounts
    .toStream()
    .map((windowedKey, value) -> {
        try {
            String originalKey = windowedKey.key();
            long windowStart = windowedKey.window().start();
            long windowEnd = windowedKey.window().end();
    
            Map<String, Object> jsonMap = Map.of(
                "key", originalKey,
                "value", value,
                "windowStart", windowStart,
                "windowEnd", windowEnd
            );
    
            // ✅ Properly convert the map to a JSON string
            String json = mapper.writeValueAsString(jsonMap);
    
            return KeyValue.pair(originalKey, json);
        } catch (Exception e) {
            e.printStackTrace();
            return KeyValue.pair("error", "{\"error\":\"serialization_failed\"}");
        }
    })
    .to("top-error-topic", Produced.with(Serdes.String(), Serdes.String()));



        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
