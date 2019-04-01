package com.test.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author:renxin.tang
 * @Desc:
 * @Date: Created in 15:52 2019/3/28
 */
public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        //  获取批处理运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath="D:\\DATA\\test.txt";//  指定输入文件路径
        String outPath = "D:\\DATA\\out.txt";
        DataSource<String> text =  env.readTextFile(inputPath);//   获取文件中的内容

        //  对数据内容进行处理   ---打平    批处理返回类型是DataSet  流处理是DataStream
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).groupBy(0).sum(1);

        // .setParallelism(1)   设置并行度为1   不然输出的结果会是一个文件夹  这个文件夹下是有多个输出的文件，文件的数量好像取决于你计算机cpu的核数
        counts.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        env.execute("Batch Word Count");

    }
    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String,Integer>> {


        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] splits = value.toLowerCase().split("\\s+");//  转成小写切分  即不区分大小写
            for (String word:splits) {
                out.collect(new Tuple2(word,1));
            }
        }
    }



}
