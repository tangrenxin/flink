package com.test.streaming;

import com.test.streaming.customSource.MyNoParalleSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:renxin.tang
 * @Desc: filter测试
 * @Date: Created in 15:49 2019/3/29
 */
public class StreamingDemoFilter {
    public static void main(String[] args) throws Exception {
        //  获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从collection中获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        SingleOutputStreamOperator<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });
        //执行filter  满足条件的数据会被留下
        SingleOutputStreamOperator<Long> filterData = num.filter(new FilterFunction<Long>() {
            //  把所有的基数过滤
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0;
            }
        });

        //   为了好比较   再写一个map
        SingleOutputStreamOperator<Long> resultdata = filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("filter后的数据：" + value);
                return value;
            }
        });


        //   每两秒钟处理一次数据
        SingleOutputStreamOperator<Long> sum = resultdata.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(1);

        String jobname = StreamingDemoFilter.class.getSimpleName();
        env.execute(jobname);

    }
}
