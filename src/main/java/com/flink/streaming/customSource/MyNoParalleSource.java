package com.flink.streaming.customSource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Author:renxin.tang
 * @Desc:自定义实现并行度为1 的source
 * 注意：SourceFunction和SourceContext都需要指定数据类型，如果不指定会报错；
 * @Date: Created in 16:07 2019/3/29
 */
public class MyNoParalleSource implements SourceFunction<Long> {
    private boolean isRunning = true;
    private long count = 1L;

    /**
     * 主要方法;启动一个数据源
     * 大部分情况下都需要在run方法中实现一个循环，这样就可以循环产生数据了
     * 模拟产生从1开始的递增数字
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        while(isRunning){

            ctx.collect(count);
            count++;
            //每秒产生一条数据
            Thread.sleep(1000);
        }
    }

    /**
     *   取消一个cancel的时候调用的方法
     */
    @Override
    public void cancel() {

        isRunning=false;
    }
}
