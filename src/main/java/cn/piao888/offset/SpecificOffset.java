package cn.piao888.offset;

import cn.piao888.aConfig.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 指定偏移量
 * auto.offset.reset 的值支持 latest, earliest, none
 * <p>
 * earliest 与 kafka-console-consumer.sh 中 的 --from-beginning   效果是一致的
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/14 1:29 下午
 * @Version 1.0
 */
public class SpecificOffset {
    public KafkaConsumer<String, String> kafkaConsumer;

    public SpecificOffset() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "specificOffset");
        this.kafkaConsumer = new KafkaConsumer(properties);

    }

    public void subject(String topic) {
        kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    public void consumer() {
        //获取一下订阅的主题-分区清空
        kafkaConsumer.poll(Duration.ofMillis(1000));
        //拿到所有订阅的主题分区数据
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        AtomicReference<TopicPartition> second = new AtomicReference<>();
        //取出主题为second的分区
        assignment.forEach(e -> {
            //获取主题为second 分区为 1的 主题分区
            if (e.topic().equals("second") && e.partition() == 1) {
                second.set(e);
            }
        });
        //### 将时间转换为 offset ###
        //声明一个map
        Map<TopicPartition, Long> stringStringMap = new HashMap<>();
        //放入 k 为取出的分区主题   v 为 当前时间的前一天  的时间戳
//        stringStringMap.put(second.get(), System.currentTimeMillis() - Duration.ofMillis(1).getSeconds());
        stringStringMap.put(second.get(), System.currentTimeMillis() - 12 * 3600 * 1000);
        //调用offsetsForTimes 放入 上面的map ，返回对应时间的 偏移量
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap = kafkaConsumer.offsetsForTimes(stringStringMap);
        //指定主题-分区 位置 开始消费数据
        OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(second.get());
        //offsetAndTimestamp有可能会返回null，这仅仅意味着在小于或等于该时间戳时没有提交的偏移量；在这种情况下，您可以使用零。
        long offset = offsetAndTimestamp == null ? 0 : offsetAndTimestamp.offset();
        kafkaConsumer.seek(second.get(), offset);

        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1));
            List<ConsumerRecord<String, String>> records = consumerRecords.records(second.get());
            records.forEach(f -> {
                System.out.printf("consumer = %s : partition = %s, offset = %d, key = %s, value = %s%n", Thread.currentThread().getName(), f.partition(), f.offset(), f.key(), f.value());
                //同步提交 消费偏移量 , 如果不提交偏移量 那么 每次 启动 他都会从上次提交的偏移量开始读取数据
                kafkaConsumer.commitSync();
            });
        }
    }

    public static void main(String[] args) {
//        System.out.println(System.currentTimeMillis());
//        System.out.println(Duration.ofHours(24).getSeconds());
//        System.out.println(System.currentTimeMillis() - Duration.ofHours(24).getSeconds());
        SpecificOffset specificOffSet = new SpecificOffset();
        specificOffSet.subject("second");
        specificOffSet.consumer();

    }
}
