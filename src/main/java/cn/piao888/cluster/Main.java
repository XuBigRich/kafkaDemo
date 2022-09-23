package cn.piao888.cluster;

import cn.piao888.cluster.consumer.ConsumerDemo;
import cn.piao888.cluster.consumer.ConsumerDemo1;
import cn.piao888.cluster.consumer.ConsumerDemo2;

/**
 * 启动集群的 主程序
 *
 * @Author： hongzhi.xu
 * @Date: 2022/7/11 4:06 下午
 * @Version 1.0
 */
public class Main {
    public static void main(String[] args) {
        ConsumerDemo consumerDemo = new ConsumerDemo("second");
        consumerDemo.subjectTopic("second");
        Thread thread = new Thread(consumerDemo, "1号消费者");
        thread.start();
        ConsumerDemo1 consumerDemo1 = new ConsumerDemo1("second");
        consumerDemo1.subjectTopic("second");
        Thread thread1 = new Thread(consumerDemo1, "2号消费者");
        thread1.start();
        ConsumerDemo2 consumerDemo2 = new ConsumerDemo2("second");
        consumerDemo2.subjectTopic("second");
        Thread thread2 = new Thread(consumerDemo2, "3号消费者");
        thread2.start();
    }
}
