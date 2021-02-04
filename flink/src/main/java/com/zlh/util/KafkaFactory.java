package com.zlh.util;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @package com.zlh.util
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
@Slf4j
public class KafkaFactory {
    public static FlinkKafkaConsumer011<String> consumerFactory(ParameterTool params) {
        String topic = params.get("kafka.topic","tz.standard.data");
        String[] topics = topic.split(",");
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, params.get("bootstrap.servers"));
        properties.setProperty("zookeeper.connect", params.get("zookeeper.connect"));
        properties.put("group.id",  params.get("group.id","dc_engine"));
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(
                Lists.newArrayList(topics), new SimpleStringSchema(), properties);
        return myConsumer;
    }
}
