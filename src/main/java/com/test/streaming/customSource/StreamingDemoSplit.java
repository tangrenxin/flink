package com.test.streaming.customSource;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author:renxin.tang
 * @Desc: split
 * 根据规则把一个数据流切分为多个数据流
 *
 * 可能在实际工作中，数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以可以根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不同的业务逻辑
 * @Date: Created in 15:49 2019/3/29
 */
public class StreamingDemoSplit {
    public static void main(String[] args) throws Exception {
        //  获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //从collection中获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParalleSource());

        SplitStream<Long> splitStream = text.split(new OutputSelector<Long>() {
            //  对流进行切分  按照数据的奇偶性进行切分
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> outPut = new ArrayList<>();
                if (value % 2 == 0) {
                    outPut.add("even");//偶数
                } else {
                    outPut.add("odd");//奇数
                }
                return outPut;
            }
        });
        //  获取 偶数
        DataStream<Long> even = splitStream.select("even");
        //  获取 奇数
        DataStream<Long> odd = splitStream.select("odd");
        //  还可以选择多个流
        DataStream<Long> all = splitStream.select("odd","even");

        all.print().setParallelism(1);

        String jobname = StreamingDemoSplit.class.getSimpleName();
        env.execute(jobname);

    }
}
