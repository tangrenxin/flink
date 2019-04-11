package com.flink.flinkfromkafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Author:renxin.tang
 * @Desc:  从kafka中读取数据
 * @Date: Created in 18:20 2019/4/4
 */
public class FlinkFromKafka {

    public static void main(String[] args) throws Exception {
        //   获取流处理运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //    checkpoint 配置
        // start a checkpoint every 1000 ms
        env.enableCheckpointing(1000);
        // advanced options:
        // set mode to exactly-once (this is the default)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // make sure 500 ms of progress happen between checkpoints
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // allow only one checkpoint to be in progress at the same time
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        String topic = "test1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","hwyuntrx:8180");

        FlinkKafkaConsumer011 kafkaConsumer011 = new FlinkKafkaConsumer011(topic,new SimpleStringSchema(),prop);

        DataStreamSource text = env.addSource(kafkaConsumer011);

        text.print().setParallelism(1);

        env.execute("FlinkFromKafka");


    }
}
