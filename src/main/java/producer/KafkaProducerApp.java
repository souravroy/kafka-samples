package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {
    public static void main(String[] args) {

        // Create a properties dictionary for the required/optional Producer config settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> myProducer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 15; i++) {
                myProducer.send(
                        new ProducerRecord<>("multipartition_replicated_topic",
                                Integer.toString(i),
                                "MyMessage " + Integer.toString(i)));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
