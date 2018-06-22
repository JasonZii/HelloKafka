package com.jason;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import kafka.javaapi.consumer.ConsumerConnector;
import org.junit.Test;

import java.util.*;

/**
 * @Author : jasonzii @Author
 * @Description : 测试producer
 * @CreateDate : 18.6.18  15:44
 */
public class TestProducer {

    /**
     * 生产者
     */
    @Test
    public void testSend(){
        Properties props = new Properties();
        //broker列表
//        props.put("metadata.broker.list", "47.105.109.51:9092");//47.105.109.51:9092
        props.put("metadata.broker.list", "192.168.25.128:9092");//47.105.109.51:9092
        //串行化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
//        props.put("zk.connectiontimeout.ms", "15000");
//        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //创建生产者配置对象
        ProducerConfig config = new ProducerConfig(props);
        //生产者
        Producer<String, String> producer = new Producer<String, String>(config);
        //发送主题，key,value
        KeyedMessage<String, String> msg = new KeyedMessage<>(
                "test3", "100", "hello world 100");
        //发送
        producer.send(msg);

        System.out.println("send over");

    }

    /**
     * 生产者
     */
    @Test
    public void testSend2(){
        Properties props = new Properties();
        //broker列表
        props.put("metadata.broker.list", "192.168.25.128:9092");
        //串行化
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //？
        props.put("request.required.acks", "1");

        //创建生产者配置对象
        ProducerConfig config = new ProducerConfig(props);

        //创建生产者
        Producer<String, String> producer = new Producer<String, String>(config);

        KeyedMessage<String, String> m1 = new KeyedMessage<String, String>(
                "test3","100" ,"hello world tomas98");

        KeyedMessage<String, String> m2 = new KeyedMessage<String, String>(
                "test3","100" ,"hello world tomas98");

        KeyedMessage<String, String> m3 = new KeyedMessage<String, String>(
                "test3","100" ,"hello world tomas98");


        List l = new ArrayList();
        l.add(m1);
        l.add(m2);
        l.add(m3);



        producer.send(l);
        System.out.println("send over!");
    }

    /**
     * 消费者
     */
    @Test
    public void testConsumer(){
        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", "192.168.25.128:2181");//192.168.25.128:2181
        // group 代表一个消费组
        props.put("group.id", "g1");
        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //创建消费者配置对象
//        ConsumerConfig config = new ConsumerConfig(props);

        //
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("test3", new Integer(1));// 一次从主题中获取一个数据
        Map<String, List<KafkaStream<byte[], byte[]>>> msgs =
                Consumer.createJavaConsumerConnector(
                        new ConsumerConfig(props)).createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("test3");
        for(KafkaStream<byte[],byte[]> stream : msgList){
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while(it.hasNext()){
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }


    }

}
