package test.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * kafka producer
 */
public class ProducerDemo {

    public static void main(String[] args) {

        //  配置信息
        Properties props = new Properties();
        //  kafka 集群   114.115.129.131  hwyuntrx
        //props.put("bootstrap.servers", "hwyuntrx:9092");
        props.put("bootstrap.servers", "hwyuntrx:8180");
        //  应答级别
        props.put("acks", "all");
        //  重试此时
        props.put("retries", 0);
        //  批量大小
        props.put("batch.size", 16384);
        //  提交延时
        props.put("linger.ms", 1);
        //  缓存
        props.put("buffer.memory", 33554432);
        //  KV的序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //   创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        for(int i = 0; i < 10; i++)
        {
            producer.send(new ProducerRecord<String, String>("test1", "消息"+i+"--"+new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date())));
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }


    
    
}
