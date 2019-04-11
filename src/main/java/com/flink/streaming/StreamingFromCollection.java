package com.flink.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author:renxin.tang
 * @Desc:Collection作为数据源
 * @Date: Created in 15:49 2019/3/29
 */
public class StreamingFromCollection {
    public static void main(String[] args) throws Exception {
        //  获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Integer> data = new ArrayList<>();
        data.add(10);
        data.add(20);
        data.add(30);
        //从collection中获取数据源
        DataStreamSource<Integer> datasource = env.fromCollection(data);

        //通过map对数据进行处理
        DataStream<Integer> map = datasource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value+1;
            }
        });
        //直接打印
        map.print().setParallelism(1);

        env.execute("StreamingFromCollection");





    }
}
