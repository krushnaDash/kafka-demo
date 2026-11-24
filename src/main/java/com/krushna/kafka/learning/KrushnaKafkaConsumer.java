package com.krushna.kafka.learning;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class KrushnaKafkaConsumer {

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        //Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"new-c1");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        
        // Disable auto-commit
        kafkaProps.put("enable.auto.commit", "false");

        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer =
                new KafkaConsumer<String,String>(kafkaProps);

        // Subscribe to the topic
		simpleConsumer.subscribe(Collections.singletonList("cocorders-uppercase"));

		try {
			while (true) {
				// Poll for new records with a timeout
				ConsumerRecords<String, String> records = simpleConsumer.poll(Duration.ofMillis(100));

				for (ConsumerRecord<String, String> record : records) {
					// Process the record
					System.out.printf("Consumed record with key %s and value %s, from partition %d and offset %d%n",
							record.key(), record.value(), record.partition(), record.offset());

					// Manually commit the offset after processing the record
					simpleConsumer.commitSync();
				}
			}
		} finally {
			// Close the consumer gracefully
			simpleConsumer.close();
		}

	}
}
