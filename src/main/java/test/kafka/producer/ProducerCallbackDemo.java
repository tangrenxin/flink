package test.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * kafka 带回调函数的 producer
 */
public class ProducerCallbackDemo {

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
            //  带回调函数的producer
            producer.send(new ProducerRecord<String, String>("test1", "消息--" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null)
                    {
                        System.out.println("发送成功：partition="+metadata.partition()+"\toffset="+metadata.offset()+" "+metadata.timestamp());
                    }else
                    {
                        System.out.println("发送失败");
                    }
                }
            });
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }


    
    
}
