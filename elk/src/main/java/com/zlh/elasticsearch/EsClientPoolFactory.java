package com.zlh.elasticsearch;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * ES连接池工厂对象
 * @Package com.dacheng.util
 * @company: dacheng
 * @Author: zlh
 * @CreateDate: 2020/4/15
 */
public class EsClientPoolFactory implements PooledObjectFactory<RestHighLevelClient> {

    private String hosts="192.168.50.98";

    private String port="9200";

    /**
     * 生产对象
     */
    @Override
    public PooledObject<RestHighLevelClient> makeObject() {
        RestHighLevelClient client = null;
        HttpHost httpHost = new HttpHost(hosts, Integer.parseInt(port), "http");
        try {
            client = new RestHighLevelClient(RestClient.builder(httpHost));

        } catch (Exception e) {
        }
        return new DefaultPooledObject<RestHighLevelClient>(client);
    }

    /**
     * 销毁对象
     */
    @Override
    public void destroyObject(PooledObject<RestHighLevelClient> pooledObject) throws Exception {
        RestHighLevelClient highLevelClient = pooledObject.getObject();
        highLevelClient.close();
    }

    @Override
    public boolean validateObject(PooledObject<RestHighLevelClient> pooledObject) {
        return false;
    }

    @Override
    public void activateObject(PooledObject<RestHighLevelClient> pooledObject) {

    }

    @Override
    public void passivateObject(PooledObject<RestHighLevelClient> pooledObject) {

    }
}
