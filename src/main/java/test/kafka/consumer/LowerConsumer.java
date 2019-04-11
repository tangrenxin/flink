package test.kafka.consumer;

import kafka.consumer.SimpleConsumer;
import kafka.javaapi.TopicMetadataRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author:renxin.tang
 * @Desc:
 * 低级API   consumer
 * 根据指定的topic  partition  offset 来获取数据
 * @Date: Created in 15:06 2019/4/4
 */
public class LowerConsumer {

    public static void main(String[] args) {
        //  参数设置
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("hwyuntrx");

        int port = 8180;

        String topic = "test1";

        int partition = 0;

        long offset = 337L;


    }
    //   flind  leader
    private String  findLeader(List<String> brokers,int port,String topic,int partition)
    {
        for (String broker : brokers) {

            //   创建获取分区leader的消费者对象
            //   host  port  延时
            SimpleConsumer getLeader = new SimpleConsumer(broker,port,1000,1024*4,"getLeder");
            //创建主体元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //   获取主题元数据返回值
            //TopicMetadataResponse metadataResponse = getLeader.send();

        }
        return "";
    }

    private void getData()
    {
        
    }

}
