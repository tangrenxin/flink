package com.test.streaming.customSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:renxin.tang
 * @Desc:自定义的数据源测试MyNoParalleSource
 * @Date: Created in 15:49 2019/3/29
 */
public class StreamingDemoWithMyNoParalleSource {
    public static void main(String[] args) throws Exception {
        //  获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从collection中获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到数据：" + value);
                return value;
            }
        });
        //   每两秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobname = StreamingDemoWithMyNoParalleSource.class.getSimpleName();
        env.execute(jobname);

    }
}
