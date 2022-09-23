package cn.piao888.cluster.consumer;

import cn.piao888.aConfig.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 创建一个消费者集群：
 * 任务：
 * 1. 消费aaa分组中的数据消息
 * 2. 手动提交偏移量
 * 3. 关掉一个集群中的消费者后查看消费者如何进行配合
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/11 4:12 下午
 * @Version 1.0
 */
public class ConsumerDemo2 implements Runnable {
    public Properties properties;
    public KafkaConsumer<String, String> consumer;
    public String topName;

    public ConsumerDemo2(String topName) {
        this.properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //配置分区分配策略    RoundRobin 策略 将所有主题分区 做hashcode 排序 ，然后分配给消费者
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        this.consumer = new KafkaConsumer(properties);
        this.topName = topName;
    }

    public void subjectTopic(String topicName) {
        consumer.subscribe(Arrays.asList(topicName));
    }


    /**
     * 订阅指定分区的数据
     *
     * @param
     */
    public void consumer() {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            //从众多消息中筛选出来 要消费的那个主题
            //如果存在值，那么使用该值 否则什么也不做
            Iterable<ConsumerRecord<String, String>> records = poll.records(topName);
            records.forEach(f -> System.out.printf("consumer = %s : partition = %s, offset = %d, key = %s, value = %s%n", Thread.currentThread().getName(), f.partition(), f.offset(), f.key(), f.value()));
        }
    }


    @Override
    public void run() {
        consumer();
    }

    public static void main(String[] args) {
        ConsumerDemo2 consumerDemo = new ConsumerDemo2("second");
        consumerDemo.subjectTopic("second");
        Thread thread = new Thread(consumerDemo);
        thread.start();
    }


}
