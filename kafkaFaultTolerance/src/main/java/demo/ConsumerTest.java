package demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Administrator on 2017/9/30.
 */
public class ConsumerTest {

    private static Properties consumerPro;

    static {
        //Consumer配置
        consumerPro = new Properties();
        consumerPro.put("bootstrap.servers", "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094");
        consumerPro.put("enable.auto.commit", "true");
        consumerPro.put("auto.offset.reset", "latest");
        consumerPro.put("zookeeper.connect", "127.0.0.1:2181");
        consumerPro.put("session.timeout.ms", "30000");
        consumerPro.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPro.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerPro.put("group.id", "test.kafka");
        consumerPro.put("auto.commit.interval.ms", "1000");
    }

    public static void main(String[] args) throws InterruptedException {
        consumerMessage();//同时消费正常信息和错误信息并进行业务处理
    }

    /**
     * 启动Consumer并消费数据
     */
    public static void consumerMessage() {
        KafkaConsumer consumer = new KafkaConsumer(consumerPro);
        consumer.subscribe(Arrays.asList("test", "wrong"));//从test和wrong两个Topic pull消息
        System.out.println(consumer);
        //消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                value = handleMessage(value);//业务处理
                JSONObject data = JSONObject.parseObject(value);
                Integer mark = data.getInteger("mark");
                if (mark == 0) {//消息处理成功
                    //持久化处理...........
                    System.out.println("插入数据库成功了:" + value);//以控制台打印模拟持久化处理
                }
                if (mark == 1) {//错误消息写入Topic(name=wrong),进行再消费
                    try {
                        ProducerTest.produceWrongMessage("wrong", value);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (mark == 2) {//错误消息再处理后仍然有错误
                    //持久化处理...........
                    //以控制台打印模拟持久化处理，项目中可以把消息持久化到数据库记录或者写入文件系统
                    System.out.println("错错错，不处理你了，把你记一下：" + value);
                }
            }
        }
    }

    /**
     * 对消费到的消息进行业务逻辑的处理
     *
     * @param value
     */
    public static String handleMessage(String value) {
        JSONObject data = JSONObject.parseObject(value);
        //模拟插入数据库操作
        int message = data.getInteger("message");
        if (message % 10 == 0) {//10的整数倍的message不插入数据库，模拟业务逻辑处理失败
            Integer mark = data.getInteger("mark");
            if (mark == null) {
                data.put("mark", 1);
            } else {
                data.put("mark", mark + 1);
            }
            System.out.println("插入数据库失败了，重新处理一次吧:"+data.toString());
        } else {
            data.put("mark", 0);//为已消费消息标记mark=0。可以对这些消息进行持久化处理。
        }
        return data.toString();
    }


}
