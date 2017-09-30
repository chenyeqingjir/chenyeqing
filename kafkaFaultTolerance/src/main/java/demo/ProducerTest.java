package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.alibaba.fastjson.JSONObject;
import java.util.Properties;

/**
 * Kafka消费端
 * Created by chenyq on 2017/9/30.
 */
public class ProducerTest {

    private static Properties producerPro;

    static {
        //Producer配置
        producerPro = new Properties();
        producerPro.put("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        producerPro.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerPro.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }
    public static void main(String[] args) throws InterruptedException {
        produceMessage("test");//生产正常信息
    }

    /**
     * Producer生产信息
     * @param topic
     * @throws InterruptedException
     */
    public static void produceMessage(String topic) throws InterruptedException {
        KafkaProducer producer = new KafkaProducer(producerPro);
        //模拟message发送
        for (int i = 1; i <= 50; i++) {
            JSONObject data = new JSONObject();
            data.put("message",i);
            ProducerRecord<String, String> pr = new ProducerRecord<>(topic, data.toJSONString());
            //发送消息
            producer.send(pr);
            Thread.sleep(1000);//发送一条message 线程休息1秒
        }
    }

    /**
     * Producer生产错误信息
     * @param topic
     * @throws InterruptedException
     */
    public static void produceWrongMessage(String topic, String value) throws InterruptedException {
        KafkaProducer producer = new KafkaProducer(producerPro);
        ProducerRecord<String, String> pr = new ProducerRecord<>(topic, value);
        producer.send(pr);
        Thread.sleep(10000);//发送一条message 线程休息1秒Value
    }

}
