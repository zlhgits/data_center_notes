package com.zlh.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.lang.System.getProperties;

/**
 * @package com.zlh.util
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/21
 */
public class KafkaSinkUtil {
    private static Producer<String, String> producer = new KafkaProducer<String, String>(getProperties());
    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.put("retries", 3);
        properties.put("acks", "0");
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //值为字符串类型
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

    public static void send(String data) throws ExecutionException, InterruptedException {
        producer.send(new ProducerRecord<String,String>("test", data)).get();
    }
}
