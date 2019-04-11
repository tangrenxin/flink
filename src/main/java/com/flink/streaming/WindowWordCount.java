package com.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 10:34 2019/3/28
 */
public class WindowWordCount {
    /**
     * 实现步骤：
     * 1、获得一个执行环境
     * 2、加载/创建初始化数据--连接socket获取输入的数据；
     * 3、指定操作数据的transaction算子
     * 4、指定把计算好的数据放在哪里
     * 5、调用execute（）触发执行程序
     *         --flink程序是延迟计算的，只有最后调用execute（）方法的时候才会真正触发执行程序。
     *         --延迟计算的好处：你可以开发复杂的程序，但是flink可以将复杂的程序转成一个plan，将plan作为一个整体单元执行。
     * @param args
     * @throws Exception
     */
    //定义socket端口号
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("没有指定参数，使用默认值9001");
            port=9001;
        }
    //1、获取流处理运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "hwyuntrx";//指定主机名/ip
        String delimiter = "\n";//指定行的结束符
    //2、加载/创建初始化数据--连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname,port,delimiter);
    //3、指定操作数据的transaction算子
        //  数据打平操作--  流处理的返回值类型是DataStream  流处理是 DataSet
        DataStream<WordCount>wordCountDataStream = text.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String value, Collector<WordCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word:splits) {
                    out.collect(new WordCount(word,1));
                }
            }
        }).keyBy("word")//根据那个属性进行分组--WordCount 中的word属性
            .timeWindow(Time.seconds(2), Time.seconds(1))//指定计算数据的窗口大小（窗口的范围）和滑动窗口大小(多久滑动一次)
            .sum("count");//这里使用sum和reduce都可以
        //  把数据打印到控制台并且设置并行度
        wordCountDataStream.print().setParallelism(1);
        //这一行一定要实现，否则程序不执行
        env.execute("Socket word Count");
    }
    public static class WordCount{
        public String word;
        public long count;
        public WordCount(){
        }
        public WordCount(String word,long count){
            this.count = count;
            this.word = word;
        }
        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

}
