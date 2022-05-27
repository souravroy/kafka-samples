package producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerGroupApp02 {
    public static void main(String[] args) {
        // Create a properties dictionary for the required/optional Subscriber config settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test-group");

        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(props);

        List<String> topics = new ArrayList<>();
        topics.add("my-big-topic");

        myConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = myConsumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // Process each consumerRecord
                    System.out.printf("Topic: %s, Partition: %d, Value: %s%n",
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.value().toUpperCase());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
