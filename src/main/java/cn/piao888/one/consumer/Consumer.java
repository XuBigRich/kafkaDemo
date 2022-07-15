package cn.piao888.one.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

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
        properties.put (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
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
            /**
             * 根据主题筛选 要处理的 主题信息
             */
            //获取消费者所有订阅的主题分区信息
            Set<TopicPartition> assignment = kafkaConsumer.assignment();
            //遍历所有消费者订阅的主题分区信息
            for (TopicPartition topicPartition : assignment) {
                //分类处理消费者 订阅的分区信息  (这样我感觉不对)
                if (topicPartition.topic().equals("aaa")) {
                    //如果分区信息为aaa ,那么取出 发送给aaa主题的消息
                    List<ConsumerRecord<String, String>> partitionRecord = records.records(topicPartition);
                    for (ConsumerRecord<String, String> consumerRecord : partitionRecord) {

                        System.out.printf("offset = %d, key = %s, value = %s%n", consumerRecord.offset(), consumerRecord.key(), consumerRecord.value());
                    }

                } else {
                    //如果是其他分区 那么就放过去
                    continue;
                }
            }
            /**
             * 所有的主题信息都消费 ，不经过筛选
             */
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }
}
