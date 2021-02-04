package com.zlh.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * @package com.zlh.elasticsearch
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/30
 */
@Slf4j
public class SourceFielterFiled {
    public static void main(String[] args) throws IOException {
        String[] fields = {"sourceId","sourceType","iocType","ioc"};
        RestHighLevelClient client = null;
        try {
            client =new RestHighLevelClient(RestClient.builder(new HttpHost("192.168.50.98",9200,"http")));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //queryString查找,是否启用 0启用 1停用
            searchSourceBuilder.query(QueryBuilders.termQuery("iocEnable", 0));
            searchSourceBuilder.size(5);
            //字段过滤，参数说明：1.包含那些字段;2.排除哪些字段
            searchSourceBuilder.fetchSource(fields,null);
            //请求ES连接
            SearchRequest searchRequest = new SearchRequest("dc_ti_*");
            searchRequest.source(searchSourceBuilder);
            //查询
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            for (SearchHit hit : searchHits) {
                //获取需要数据
                String sourceValue = hit.getSourceAsString();
                JSONObject jsonObject = JSONObject.parseObject(sourceValue);
                Integer sourceId = jsonObject.getInteger("sourceId");
                String sourceType = jsonObject.getString("sourceType");
                String iocType = jsonObject.getString("iocType");
                if (iocType != null) {
                    iocType = iocType.toLowerCase();
                }
                String ioc = jsonObject.getString("ioc");
                String iocKey = sourceId + "_" + iocType + "_" + ioc;
                System.out.println(iocKey);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            client.close();
        }
    }
}
