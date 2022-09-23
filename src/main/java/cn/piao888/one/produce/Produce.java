package cn.piao888.one.produce;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * org.apache.kafka.clients.producer.KafkaProducer -- 生产者
 * org.apache.kafka.clients.producer.ProducerConfig -- 生产者配置
 * org.apache.kafka.clients.producer.ProducerRecord -- 消息
 *
 * @author 许鸿志
 * @since 2022/6/13
 */
public class Produce {
    //生产者主体
    public KafkaProducer<String, String> kafkaProducer;
    //生产者配置
    public Properties producerProperties;

    public Produce() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.100.130:9092,172.16.100.130:9091,172.16.100.130:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer kafkaProducer = new KafkaProducer(properties);
        this.producerProperties = properties;
        this.kafkaProducer = kafkaProducer;
    }

    public Produce(KafkaProducer kafkaProducer, Properties producerConfig) {
        this.kafkaProducer = kafkaProducer;
        this.producerProperties = producerConfig;
    }

    public static void main(String[] args) {
        Produce produce = new Produce();
        KafkaProducer<String, String> kafkaProducer = produce.kafkaProducer;
        while (true){
            kafkaProducer.send(new ProducerRecord<String, String>("aaa", "bb"));
        }

    }
}
