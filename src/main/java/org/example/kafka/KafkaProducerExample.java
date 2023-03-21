package org.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerExample {

    public static void main(String[] args) {

          final String TOPIC ="TestTopic";
          String message = UUID.randomUUID().toString();

        Producer<String,String> producer = new KafkaProducer<String, String>(loadProperties());
        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC,message);
        producer.send(record);
        System.out.println("Message sent to Kafka on Topic : "+TOPIC);
        producer.close();
    }

    public static Properties loadProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }


}
