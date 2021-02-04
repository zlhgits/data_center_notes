package com.zlh.collect;

import com.alibaba.fastjson.JSONObject;
import com.zlh.function.EsRequestFailureFunction;
import com.zlh.util.ElasticSearchSinkUtil;
import com.zlh.util.FlinkEnvUtil;
import com.zlh.util.KafkaFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.MalformedURLException;
import java.util.List;

/**
 * 消费kafka数据到es
 * @package com.zlh.collect
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
@Slf4j
public class CollectKafkaToEs {
    public static void main(String[] args) {
        /**
         * job.name、parallelism、bootstrap.servers、zookeeper.connect、kafka.topic、es.host、es.index
         */
        final ParameterTool params= ParameterTool.fromArgs(args);
        String index = params.get("es.index");
        int parallelism = params.getInt("parallelism",3);
        String jobName = params.get("job.name","CollectKafkaToEs");
        //flink运行环境
        StreamExecutionEnvironment executionEnvironment = FlinkEnvUtil.getFlinkEnv(params);
        FlinkKafkaConsumer011<String> kafkaConsumer011 = KafkaFactory.consumerFactory(params);

        //消费kafka告警数据
        DataStreamSource<String> kafkaStream = executionEnvironment.addSource(kafkaConsumer011).setParallelism(parallelism);

        //sink到es
        int bulkSize = 1000;
        //esSink
        List<HttpHost> httpHosts;
        try {
            httpHosts = ElasticSearchSinkUtil.getEsAddresses(params.get("es.host","127.0.0.1:9200"));
            ElasticSearchSinkUtil.addSink(httpHosts,bulkSize,parallelism,kafkaStream, (ElasticsearchSinkFunction<String>) (s, runtimeContext, indexer) -> {
                JSONObject data = JSONObject.parseObject(s);

                IndexRequest indexRequest = new IndexRequest(index).type("_doc").source(data, XContentType.JSON);
                // 数据添加到RequestIndex中
                indexer.add(indexRequest);
            },new EsRequestFailureFunction(),"StandarEsSink");
            executionEnvironment.execute(jobName);
        } catch (MalformedURLException e) {
            log.error("ES url 格式error:{ }",e);
        } catch (Exception e) {
            log.error("启动情报碰撞引擎error:{}",e);
        }
    }
}
