package org.example.processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {
    private static final String APPLICATION_NAME = "processor-application";
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String STEAM_LOG = "stream_log";
    private static final String STREAM_LOG_FILTER = "stream_log_filter";

    public static void run() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();
        topology.addSource("Source", STEAM_LOG)
                .addProcessor("Process", () -> new FilterProcessor(), "Source")
                .addSink("Sink", STREAM_LOG_FILTER, "Process");

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
