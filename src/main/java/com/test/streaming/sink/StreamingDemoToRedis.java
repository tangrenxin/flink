package com.test.streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;


/**
 * @Author:renxin.tang
 * @Desc: 功能：接收Socket数据，把数据保存到Redis中
 *
 * 使用list类型
 *
 * lpush list key value
 *
 *
 * @Date: Created in 14:35 2019/4/1
 */
public class StreamingDemoToRedis {

    public static void main(String[] args) throws Exception{
        //   获取流处理运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  获取数据源--cong socket 获取数据
        DataStreamSource<String> text = env.socketTextStream("hwyuntrx", 9001, "\n");
        //    命令     key    value
        //   lpush  l_words   word
        //  对数据进行组装
        DataStream<Tuple2<String, String>> l_wordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<String, String>("l_words", value);
            }
        });
        /**
         * 添加Redis sink 需要做三个事儿：
         * 1、创建自定义RedisMapper类，这个类需要实现RedisMapper，用来封装使用的Redis命令、获取key和获取值。在下面创建Redis sink的时候会用到
         * 2、创建Redis配置，需要设置Redis主机和端口，你得让程序知道Redis在哪
         * 3、创建Redis sink ，创建的时候需要传入上面的配置个实体类
         */
        //  创建 Redis 配置
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("hwyuntrx").setPort(6379).build();
        //   创建Redis sink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());
        l_wordsData.addSink(redisSink);
        env.execute("StreamingDemoToRedis");

    }
    //  创建自定义RedisMapper类  这个地方注意Tuple2的包不要导入错   我用的Java实现，结果错导成了Scala包
    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>>{

        //  指定要使用的命令
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }
        //  表示从接收的数据中 获取需要操作的 Redis  key
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }
        //  表示从接收的数据中 获取需要操作的 Redis value
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }


}
