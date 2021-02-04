package com.zlh.util;

import com.zlh.function.EsRequestFailureFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @package com.zlh.util
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
public class ElasticSearchSinkUtil {

    /**
     * es sink
     *
     * @param <T>
     * @param hosts es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism 并行数
     * @param data 数据
     * @param func
     * @param esRequestFailureFunction
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   DataStream<T> data, ElasticsearchSinkFunction<T> func, EsRequestFailureFunction esRequestFailureFunction, String nameUid) {

        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<T>(hosts, func);
        //开启重试机制
        esSinkBuilder.setBulkFlushBackoff(true);
        //批量写入时的最大写入条数
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
        //失败重试的次数
        esSinkBuilder.setBulkFlushBackoffRetries(3);
        //进行重试的时间间隔
        esSinkBuilder.setBulkFlushBackoffDelay(1000);
        //每次刷新的时间间隔
        esSinkBuilder.setBulkFlushInterval(5000L);
        //EXPONENTIAL 指数型，表示多次重试之间的时间间隔按照指数方式进行增长。eg:2 -> 4 -> 8
        //CONSTANT 常数型（表示多次重试之间的时间间隔为固定常数）
        esSinkBuilder.setBulkFlushBackoffType(ElasticsearchSinkBase.FlushBackoffType.EXPONENTIAL);
        //错误信息
        esSinkBuilder.setFailureHandler(esRequestFailureFunction);
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism).name(nameUid).uid(nameUid);
    }

    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<HttpHost>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }
}
