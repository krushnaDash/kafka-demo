package com.krushna.kafka.stream;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KafkaStreamsExample {

    public static void main(String[] args) {
        // Step 1: Define the configuration properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Step 2: Define the processing topology
        StreamsBuilder builder = new StreamsBuilder();

        // Step 3: Create a stream from the input topic
        KStream<String, String> sourceStream = builder.stream("cocorders");

        // Step 4: Process the stream (convert to uppercase)
        KStream<String, String> uppercasedStream = 
        		
        		sourceStream.mapValues((ValueMapper<String, String>) String::toUpperCase)
        		.mapValues( value -> {System.out.println(value); return value; });
        		

        // Step 5: Write the processed stream to the output topic
        uppercasedStream.to("cocorders-uppercase");

        // Step 6: Build the topology
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Step 7: Start the streams application
        streams.start();

        // Add shutdown hook to close the streams application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
