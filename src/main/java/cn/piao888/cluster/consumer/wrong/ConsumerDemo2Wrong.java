package cn.piao888.cluster.consumer.wrong;

import cn.piao888.aConfig.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 错误示范 ：
 * 这个地方 必须先拉取一下 发到这个分区的消息 ，才可以知道当前分区订阅了哪个主题  所以这个是错的
 * 所以不可以先获取消费主题与分区，再获取消息
 * 当第一次获取消息的时候 就会获取要消费的分区和主题信息
 * 创建一个消费者集群：
 * <p>
 * 任务：
 * 1. 消费aaa分组中的数据消息
 * 2. 手动提交偏移量
 * 3. 关掉一个集群中的消费者后查看消费者如何进行配合
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/11 4:12 下午
 * @Version 1.0
 */
public class ConsumerDemo2Wrong implements Runnable {
    public Properties properties;
    public KafkaConsumer<String, String> consumer;
    public String topName;

    public ConsumerDemo2Wrong(String topName) {
        this.properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        this.consumer = new KafkaConsumer(properties);
        this.topName = topName;
    }

    public void subjectTopic(String topicName) {
        consumer.subscribe(Arrays.asList(topicName));
    }

    public Set<TopicPartition> getSubjectAllTopicPartition(KafkaConsumer kafkaConsumer) {
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        return assignment;
    }

    /**
     * 订阅指定分区的数据
     *
     * @param topicPartition
     */
    public void consumer(TopicPartition topicPartition) {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            List<ConsumerRecord<String, String>> records = poll.records(topicPartition);
            records.forEach(e -> {
                System.out.printf("offset = %d, key = %s, value = %s%n", e.offset(), e.key(), e.value());
            });
        }
    }

    @Override
    public void run() {
        //这个地方 必须先拉取一下 发到这个分区的消息 ，才可以知道当前分区订阅了哪个主题  所以这个是错的
        Set<TopicPartition> subjectAllTopicPartition = getSubjectAllTopicPartition(consumer);
        Iterator<TopicPartition> iterator = subjectAllTopicPartition.iterator();
        TopicPartition target = null;
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            if (topicPartition.topic().equals(topName)) {
                target = topicPartition;
            }
        }
        //通过Optional包装一下
        Optional<TopicPartition> optional = Optional.ofNullable(target);
        //如果存在值，那么使用该值 否则什么也不做
        optional.ifPresent(this::consumer);
    }

    public static void main(String[] args) {
        ConsumerDemo2Wrong consumerDemo = new ConsumerDemo2Wrong("aaa");
        consumerDemo.subjectTopic("aaa");
        Thread thread = new Thread(consumerDemo);
        thread.start();
    }


}