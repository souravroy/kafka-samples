package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerAssignApp {
    public static void main(String[] args) {
        // Create a properties dictionary for the required/optional Subscriber config settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<>(props);

        List<TopicPartition> topicPartitions = new ArrayList<>();

        TopicPartition topicPartition0 = new TopicPartition("my-topic", 0);
        TopicPartition topicPartition2 = new TopicPartition("my-other-topic", 2);

        topicPartitions.add(topicPartition0);
        topicPartitions.add(topicPartition2);

        myConsumer.assign(topicPartitions);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = myConsumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    // Process each consumerRecord
                    System.out.printf("Topic: %s, Partition: %d, Key: %s, Value: %s%n",
                            consumerRecord.topic(),
                            consumerRecord.partition(),
                            consumerRecord.key(),
                            consumerRecord.value());
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            myConsumer.close();
        }
    }
}
