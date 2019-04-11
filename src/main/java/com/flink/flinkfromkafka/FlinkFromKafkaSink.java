package com.flink.flinkfromkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @Author:renxin.tang
 * @Desc:  从kafka中读取数据
 * @Date: Created in 18:20 2019/4/4
 */
public class FlinkFromKafkaSink {

    public static void main(String[] args) throws Exception {
        //   获取流处理运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String topic = "test1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hwyuntrx:8180");

        FlinkKafkaConsumer011 kafkaConsumer011 = new FlinkKafkaConsumer011(topic,new SimpleStringSchema(),prop);

        DataStreamSource text = env.addSource(kafkaConsumer011);

        //text.print().setParallelism(1);

        String broker = "hwyuntrx:8180";
        String topicSink = "test2";

        FlinkKafkaProducer011<String> stringFlinkKafkaProducer011 = new FlinkKafkaProducer011<>(broker, topicSink, new SimpleStringSchema());
        text.addSink(stringFlinkKafkaProducer011);
        env.execute("stringFlinkKafkaProducer011");
    }




}
