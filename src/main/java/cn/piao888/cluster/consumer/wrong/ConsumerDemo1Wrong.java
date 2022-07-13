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
 * 创建一个消费者集群：
 * 任务：
 * 1. 消费aaa分组中的数据消息
 * 2. 手动提交偏移量
 * 3. 关掉一个集群中的消费者后查看消费者如何进行配合
 * <p>
 * 错误示范：   想要通过TopicPartition 筛选主题分区，导致后来的 主题-分区 ，覆盖掉了前面的主题-分区
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/11 4:12 下午
 * @Version 1.0
 */
public class ConsumerDemo1Wrong implements Runnable {
    public Properties properties;
    public KafkaConsumer<String, String> consumer;
    public String topName;

    public ConsumerDemo1Wrong(String topName) {
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
     * @param
     */
    public void consumer() {
        while (!Thread.interrupted()) {
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            //从众多消息中筛选出来 要消费的那个主题
            TopicPartition topicPartition = getTopicPartitions();
            Optional<TopicPartition> optional = Optional.ofNullable(topicPartition);
            //如果存在值，那么使用该值 否则什么也不做
            optional.ifPresent(e -> {
                List<ConsumerRecord<String, String>> records = poll.records(e);
                records.forEach(f -> System.out.printf("consumer = %s : partition = %s, offset = %d, key = %s, value = %s%n", Thread.currentThread().getName(), f.partition(), f.offset(), f.key(), f.value()));
            });

        }
    }

    public TopicPartition getTopicPartitions() {
        //获取当前对象 消费者 获得的消息里面， 分别来自于那些分区
        Set<TopicPartition> subjectAllTopicPartition = getSubjectAllTopicPartition(consumer);
        Iterator<TopicPartition> iterator = subjectAllTopicPartition.iterator();
        //这个地方是  主题-分区  而不是主题,所以下面筛选的 主题是不对的。
        TopicPartition target = null;
        while (iterator.hasNext()) {
            TopicPartition topicPartition = iterator.next();
            //如果一个主题下有多个分区，那么 当接收到信息后， 最后面的分区 总该会替代前面的分区信息。(错误)
            if (topicPartition.topic().equals(topName)) {
                //这样 target对象就会被新的 主题-分区 替代，老的 主题-分区  就永远不会被消费到
                target = topicPartition;
            }
        }
        //通过Optional包装一下
        return target;
    }

    @Override
    public void run() {
        consumer();
    }

    public static void main(String[] args) {
        ConsumerDemo1Wrong consumerDemo = new ConsumerDemo1Wrong("second");
        consumerDemo.subjectTopic("second");
        Thread thread = new Thread(consumerDemo);
        thread.start();
    }


}
