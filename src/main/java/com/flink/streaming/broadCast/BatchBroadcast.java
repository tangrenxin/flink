package com.flink.streaming.broadCast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author:renxin.tang
 * @Desc:
 * 广播变量
 * 需求：flink会从数据源中获取用户的姓名，最终要把用户的姓名和年龄信息打印出来。
 * 分析：
 * flink只能从source中获取用户的姓名
 * 所以就需要在中的的map处理的时候 获取用户的年龄信息，将姓名和年龄拼一块返回进行输出
 *
 * 把用户的关系数据集使用广播变量进行处理
 *
 * 如果多个算子想要使用这个广播变量，那么这些map都需要注册广播变量，即.withBroadcastSet(toBroadcast, "toBroadcast")
 * 注意：
 *
 * 1：广播出去的变量存在于每个节点的内存中，所以这个数据集不能太大。因为广播出去的数据，会常驻内存，除非程序执行结束。
 *
 * 2：广播变量在初始化广播出去以后不支持修改，这样才能保证每个节点的数据都是一致的。
 * @Date: Created in 15:02 2019/4/2
 */
public class BatchBroadcast {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //1：准备需要广播的数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("zs",18));
        broadData.add(new Tuple2<>("ls",20));
        broadData.add(new Tuple2<>("ww",17));
        DataSource<Tuple2<String, Integer>> tupleData = env.fromCollection(broadData);

        //1.1:处理需要广播的数据,把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄,最终是根据传过来的姓名得到姓名跟年龄信息。
        //  list 转换成  map
        DataSet<HashMap<String, Integer>> toBroadcast  = tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });
        //    -------------开始使用
        //  获取源数据  测试使用  方便
        DataSource<String> data = env.fromElements("zs", "ls", "ww");

        // 数据处理，这个地方要使用RichMapFunction  里面有个open方法   在这里面才能获取广播变量
        //  open方法 只会执行一次，可以在里面实现初始化的功能
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<HashMap<String, Integer>>();
            HashMap<String, Integer> allMap = new HashMap<String, Integer>();
            /**
             * 这个方法只会执行一次
             * 可以在这里实现一些初始化的功能
             * 所以，就可以在open方法中获取广播变量数据
             * */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //3:获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("toBroadcast");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }
            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + " -- " + age;
            }
        }).withBroadcastSet(toBroadcast, "toBroadcast");//执行广播变量的操作
        result.print();
    }



}
