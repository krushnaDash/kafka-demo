package com.krushna.kafka.learning;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class URKafkaProducer {
	
	public static void main(String[] args) {
		// Set up the producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1301504412-1-1758430482.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-2-1758430485.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-3-1758430488.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-4-1758430491.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-5-1758430494.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-6-1758430497.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-7-1758430500.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-8-1758430503.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093,kafka-1301504412-9-1758430506.scus.kafka-taas-shared7-stg.ms-df-messaging.stg-az-southcentralus-10.prod.us.walmart.net:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        
        props.put("kafka.ssl.taas.security_protocol","SSL");
        props.put("kafka.ssl.taas.ssl_keystore_location","/Users/k0d03gd/Library/CloudStorage/OneDrive-WalmartInc/STFC/URS/Secret/JKS/APM0017936.stfcurnormalizer.dev.kafka-clients.walmart.net.jks");
        props.put("kafka.ssl.taas.ssl_keystore_password","Stfcnextgen@2024");
        props.put("kafka.ssl.taas.ssl_truststore_location","/Users/k0d03gd/Library/CloudStorage/OneDrive-WalmartInc/STFC/URS/Secret/JKS/APM0017936.stfcurnormalizer.dev.kafka-clients.walmart.net.jks");
        props.put("kafka.ssl.taas.ssl_truststore_password", "Stfcnextgen@2024");
        props.put("kafka.ssl.taas.ssl_key_password","Stfcnextgen@2024");
        		
		
       KafkaProducer<String, String> producer= new KafkaProducer<String, String>(props);
       
       // Define the partition key (e.g., order ID)
       String partitionKey = "Test_1001";
       

		
		producer.send(new ProducerRecord<>("APM0017936-stfc-ur-DEV-unified-recon", partitionKey, "{\n"
				+ "  \"headers\": {\n"
				+ "    \"audit\": \"\\u0000�ڧƨd\\u0000\\u0016\\u0000\\u0002.urn:wm:publisher:stfc.9\\u0000\\u0000\\u0000\\u0006WUS\\u0000\\u0000801J5269XM561J3CHRDTNZ5V00E_1\\u0002\\u0000�8�\\u0001urn:bkey:wm:us:1723429353093:fdfevents_200012195603942-202408091534379633182655ByfMWJG6-2b38e43c-e6a8-45ed-b87c-e811ddd90d66\\u0016ur-etl-jobs\\u00061.0$com.walmart.memory\\u0000\\u0000\\u0000\\u0000\\u0002�\\u0001sha-256://7ca04c1d1949935601875df6ab1a179b670e90a8605805af7212c337749fb7c2\\u0000\\u0000\\u0000\\u0000\\u0000\"\n"
				+ "  },\n"
				+ "  \"value\": {\n"
				+ "    \"orderId\": \"Test_1001\",\n"
				+ "    \"invoiceId\": \"202408091534379633182655ByfMWJG6\",\n"
				+ "    \"payloadType\": \"ACCOUNTING\",\n"
				+ "    \"payload\": {\n"
				+ "      \"amount\": 42.53,\n"
				+ "      \"accountingCode\": \"1296002\",\n"
				+ "      \"paymentHandle\": \"2b38e43c-e6a8-45ed-b87c-e811ddd90d66\",\n"
				+ "      \"postingDate\": \"2024-08-09\",\n"
				+ "      \"provider\": \"ACCULYNK_PINLESS\",\n"
				+ "      \"cardType\": \"ACCULYNK_PINLESS\"\n"
				+ "    }\n"
				+ "  }\n"
				+ "}"));
		
		
		producer.close();
		System.out.println(producer.metrics());
			
	

	}
	
	
}
