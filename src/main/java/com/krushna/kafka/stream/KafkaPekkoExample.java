package com.krushna.kafka.stream;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.kafka.javadsl.Producer;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.RunnableGraph;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaPekkoExample {

    public static void main(String[] args) {

        // Step 1: Create an Actor System
        final ActorSystem system = ActorSystem.create("KafkaPekkoExample");
        final Materializer materializer = Materializer.createMaterializer(system);

        // Step 2: Define Kafka Consumer Settings
        final ConsumerSettings<String, String> consumerSettings =
                ConsumerSettings.create(system, new StringDeserializer(), new StringDeserializer())
                        .withBootstrapServers("localhost:9092")
                        .withGroupId("example-group")
                        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Step 3: Define Kafka Producer Settings
        final ProducerSettings<String, String> producerSettings =
                ProducerSettings.create(system, new StringSerializer(), new StringSerializer())
                        .withBootstrapServers("localhost:9092");

        // Step 4: Define the Source (Kafka Consumer)
        final Source<ConsumerRecord<String, String>, Consumer.Control> source =
                Consumer.plainSource(consumerSettings, org.apache.pekko.kafka.Subscriptions.topics("cocorders"));

        // Step 5: Define the Sink (Kafka Producer)
        final Sink<ProducerRecord<String, String>, CompletionStage<org.apache.pekko.Done>> sink =
                Producer.plainSink(producerSettings);

        // Step 6: Build the Stream Processing Logic
        final RunnableGraph<Consumer.Control> stream = source
                .map(record -> {
                    String uppercasedValue = record.value().toUpperCase();
                    return new ProducerRecord<>("cocorders-uppercase", record.key(), uppercasedValue);
                })
                .toMat(sink, Keep.left());

        // Step 7: Run the Stream
        final Consumer.Control control = stream.run(materializer);

        // Step 8: Ensure graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            control.shutdown();
            system.terminate();
        }));
    }
}
