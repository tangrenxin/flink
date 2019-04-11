package com.flink.batch.accumulators;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

/**
 * @Author:renxin.tang
 * @Desc:
 * 全局累加器
 * counter计数器
 * 需求：
 * 计算map处理数据的数量
 *
 * 只有在任务执行结束后才能获取到最后的结果
 *
 * @Date: Created in 15:49 2019/4/2
 */
public class AccumulatorTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d", "e");

        DataSet<String> result = dataSource.map(new RichMapFunction<String, String>() {
            //1.创建累加器
            private IntCounter numberline = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //注册累加器
                getRuntimeContext().addAccumulator("numberline", this.numberline);
            }

            @Override
            public String map(String s) throws Exception {
                this.numberline.add(1);//  累加器加 1
                return s;
            }
        }).setParallelism(4);

        //   批处理不能用 print
        // result.print();
        //  sink    写入文件
        result.writeAsText("d:\\DATA\\count2");
        JobExecutionResult test = env.execute("test");
        //   在程序执行完之后才能获取全局的count
        int num = test.getAccumulatorResult("numberline");
        System.out.println("num:"+num);
    }


}
