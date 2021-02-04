package com.zlh.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * ES连接池工具类
 * @Package com.dacheng.util
 * @company: dacheng
 * @Author: zlh
 * @CreateDate: 2020/4/15
 */
@Slf4j
public class ElasticPoolUtil {

    private static EsClientPoolFactory esClientPoolFactory = new EsClientPoolFactory();
    /**
     * 对象池配置类，不设置则采用默认配置
     */
    private static GenericObjectPoolConfig<RestHighLevelClient> poolConfig = new GenericObjectPoolConfig<>();

    private static GenericObjectPool<RestHighLevelClient> clientPool=new GenericObjectPool<>(esClientPoolFactory,poolConfig);

    /**
     * 获得连接
     * @return
     * @throws Exception
     */
    public RestHighLevelClient getClient() throws Exception {
        // 从池中取一个对象
        RestHighLevelClient client = clientPool.borrowObject();
        return client;
    }

    /**
     * 释放连接
     * @param client
     */
    public void returnClient(RestHighLevelClient client) {
        // 使用完毕之后，归还对象
        clientPool.returnObject(client);
        //关闭连接池
        clientPool.close();
    }
}
