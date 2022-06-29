package cn.piao888.one.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author 许鸿志
 * @since 2022/6/13
 */
public class Consumer {
    private KafkaConsumer<String, String> kafkaConsumer;
    private Properties properties;

    public Consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.100.130:9092,172.16.100.130:9091,172.16.100.130:9093");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        this.properties = properties;
        this.kafkaConsumer = kafkaConsumer;
    }

    public Consumer(KafkaConsumer<String, String> kafkaConsumer, Properties properties) {
        this.kafkaConsumer = kafkaConsumer;
        this.properties = properties;
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        KafkaConsumer<String, String> kafkaConsumer = consumer.kafkaConsumer;
        kafkaConsumer.subscribe(Arrays.asList("aaa"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
