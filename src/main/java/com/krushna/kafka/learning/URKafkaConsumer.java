package com.krushna.kafka.learning;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;


public class URKafkaConsumer {

    public static void main(String[] args) {

        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"kafka-1301504412-1-1758430482.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-2-1758430485.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-3-1758430488.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-4-1758430491.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-5-1758430494.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-6-1758430497.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-7-1758430500.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-8-1758430503.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-9-1758430506.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        
        kafkaProps.put("security.protocol","SSL");
        kafkaProps.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,"/Users/k0d03gd/Library/CloudStorage/OneDrive-WalmartInc/STFC/URS/Secret/JKS/APM0017936.stfcurnormalizer.dev.kafka-clients.walmart.net.jks");
        kafkaProps.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,"Stfcnextgen@2024");
        kafkaProps.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,"/Users/k0d03gd/Library/CloudStorage/OneDrive-WalmartInc/STFC/URS/Secret/JKS/APM0017936.stfcurnormalizer.dev.kafka-clients.walmart.net.jks");
        kafkaProps.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "Stfcnextgen@2024");
        kafkaProps.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,"Stfcnextgen@2024");
     

        //Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,"new-c1");

        //Set to consume from the earliest message, on start when no offset is
        //available in Kafka
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        
        // Disable auto-commit
        kafkaProps.put("enable.auto.commit", "false");

        //Create a Consumer
        KafkaConsumer<String, String> simpleConsumer =
                new KafkaConsumer<>(kafkaProps);

        // Subscribe to the topic
		simpleConsumer.subscribe(Collections.singletonList("APM0017936-stfc-ur-DEV-unified-recon"));

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
