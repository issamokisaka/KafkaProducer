package br.org.lab.kafka_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Properties;

public class KafkaProducer {

    private Properties properties;

    public ProducerKafka() {
        properties = new Properties();
        properties.put("bootstrap.servers", "node01:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    }

    public static void main(String args[]) throws InterruptedException {
        ProducerKafka pk = new ProducerKafka();

        Thread.currentThread().sleep(5000);
        pk.send();

        Thread.currentThread().sleep(5000);
        pk.send();

    }

    public void send() {

        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        ProducerRecord<String, String> record = new ProducerRecord("test", "DF", "Brasília");

        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.partition());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
