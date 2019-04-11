package test.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 19:52 2019/4/3
 */
public class ConsumerDemo {
    public static void main(String[] args) {
        // 配置信息
        Properties props = new Properties();
        //
        props.put("bootstrap.servers", "hwyuntrx:8180");
//        props.put("bootstrap.servers", "10.1.1.29:9092");
        // 消费者组id
        props.put("group.id", "tests");
        // 是否自动提交  设置自动提交offset
        props.put("enable.auto.commit", "true");
        //   提交延时
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //   订阅topic   可以同时订阅多个 topic
        consumer.subscribe(Arrays.asList("test2","test3"));//topic-trx   "test1",
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(10);
            for (ConsumerRecord<String, String> record : records)
            {
                System.out.printf("offset = %d, topic = %s, value = %s,接收时间：%s", record.offset(), record.topic(), record.value(),String.valueOf(new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date())));
                System.out.println();
            }


        }
    }

}
