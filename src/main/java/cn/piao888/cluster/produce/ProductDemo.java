package cn.piao888.cluster.produce;

import cn.piao888.aConfig.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;

/**
 * @Author： hongzhi.xu
 * @Date: 2022/7/11 4:11 下午
 * @Version 1.0
 */
public class ProductDemo {
    public Properties properties;
    public KafkaProducer<String, String> producer;

    public ProductDemo() {
        this.properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer(properties);
    }

    public void sendMessage(String topic, String message) {
        ProducerRecord producerRecord = new ProducerRecord(topic, message);
        producer.send(producerRecord, (metadata, exception) -> {
            Optional<Exception> exception1 = Optional.ofNullable(exception);
            exception1.ifPresent(e -> System.out.println(e));
            System.out.printf("offset = %d, topic = %s, partition = %s, message = %s%n", metadata.offset(), metadata.topic(), metadata.partition(),message);


        });
    }

    public void close() {
        producer.close();
    }


    public static void main(String[] args) throws InterruptedException {
        ProductDemo productDemo = new ProductDemo();
        int i = 0;
        while (true) {
            Thread.sleep(1000);
            productDemo.sendMessage("second", i+++"");
        }
    }
}
