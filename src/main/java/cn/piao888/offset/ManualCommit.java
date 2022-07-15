package cn.piao888.offset;

import cn.piao888.aConfig.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 手动提交Offset
 * 在kafka中当消费者查找不到所记录的消费位移时，会根据auto.offset.reset的配置，决定从何处消费。
 * auto.offset.reset = earliest | latest | none
 * earliest：
 * 自动将偏移量重置为最早的偏移量，--from-beginning。
 * latest：
 * （默认值）：自动将偏移量重置为最新偏移量
 * none：
 * 如果未找到消费者组的先前偏移量，则向消费者抛出异常。
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/13 8:42 下午
 * @Version 1.0
 */
public class ManualCommit {
    private KafkaConsumer<String, String> consumer;

    public ManualCommit() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "manualCommit");
        consumer = new KafkaConsumer(properties);
    }

    //不可以重复调用consumer.subscribe 后来的 会覆盖原先的
    public ManualCommit subject(String topic) {
        consumer.subscribe(Arrays.asList(topic));
        return this;
    }


    public void consumer() {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1));
            consumerRecords.forEach(f -> {
                System.out.printf("consumer = %s : partition = %s, offset = %d, key = %s, value = %s%n", Thread.currentThread().getName(), f.partition(), f.offset(), f.key(), f.value());
                //同步提交 消费偏移量 , 如果不提交偏移量 那么 每次 启动 他都会从上次提交的偏移量开始读取数据
//                consumer.commitSync();
            });
        }
    }


    public static void main(String[] args) {
        System.out.println(Math.abs("manualCommit".hashCode()) % 50);
        ManualCommit manualCommit = new ManualCommit();
        manualCommit.subject("first").subject("second");
        manualCommit.consumer();
    }

}
