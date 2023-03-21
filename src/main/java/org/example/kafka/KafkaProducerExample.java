package org.example.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerExample {

    public static void main(String[] args) {

        final String TOPIC = "TestTopic";
        String message = UUID.randomUUID().toString();

        Producer<String, String> producer = new KafkaProducer<String, String>(loadProperties());
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, UUID.randomUUID().toString().split("-")[0], message);
        Future<RecordMetadata> result = producer.send(record);
        try {
            System.out.println("Message sent to Kafka on Topic : " + TOPIC + " result : " + result.get().offset());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        producer.close();
    }

    public static Properties loadProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


}
