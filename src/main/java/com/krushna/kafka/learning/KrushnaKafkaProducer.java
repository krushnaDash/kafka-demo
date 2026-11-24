package com.krushna.kafka.learning;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KrushnaKafkaProducer {
	
	public static void main(String[] args) {
		// Set up the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());		
		
       KafkaProducer<String, String> producer= new KafkaProducer<String, String>(props);
       
       // Define the partition key (e.g., order ID)
       String partitionKey = "order_";
       
		int i = 1001;

		while (true) {
			producer.send(new ProducerRecord<>("cocorders", partitionKey + i, " order JSON " + i));
			++i;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				producer.close();
			}
		}
	

	}
	
	
}
