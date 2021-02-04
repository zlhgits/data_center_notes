package com.zlh.elasticsearch;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Date;
import java.util.Map;

/**
 * @package com.zlh.elasticsearch
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/9/1
 */
@Slf4j
public class QueryBuildTest {
    public static void main(String[] args) {
        QueryBuildTest queryBuildTest = new QueryBuildTest();
        String index = "dc_est_data";
        //QueryBuil的query叠加使用
        queryBuildTest.queryTest1(index);
    }

    /**
     * 两次query之间会覆盖,多个条件必须使用bool查询
     * searchSourceBuilder.query(QueryBuilders.queryStringQuery(params));
     * searchSourceBuilder.query(QueryBuilders.rangeQuery("timestamp").gt(1592664200).lt(1592666900));
     * 而且没有查询关系，不能准确描述条件之间是and or 还是其他关系
     * @param index
     */
   private void queryTest1(String index){
       ElasticPoolUtil elasticPoolUtil = new ElasticPoolUtil();
       RestHighLevelClient client = null;
       try {
           client = elasticPoolUtil.getClient();
           SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
           BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
           //queryString查找,连接词AND必须大写
           String params = "itemid:cpu1 AND level:3";
           boolQueryBuilder.must(QueryBuilders.queryStringQuery(params));
           //范围查找
           boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").gt(1592664200).lt(1592666900));

           searchSourceBuilder.query(boolQueryBuilder);
           searchSourceBuilder.size(100);
           //请求ES连接
           SearchRequest searchRequest = new SearchRequest(index).source(searchSourceBuilder);
           //查询
           SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
           SearchHit[] searchHits = searchResponse.getHits().getHits();
           for (SearchHit hit:searchHits){
               System.out.println(hit.getSourceAsString());
           }
       }catch (Exception e){
           e.printStackTrace();
       }finally {
           elasticPoolUtil.returnClient(client);
       }
   }

    private void aggQueryTest2(String index){
        ElasticPoolUtil elasticPoolUtil = new ElasticPoolUtil();
        RestHighLevelClient client = null;
        try {
            client = elasticPoolUtil.getClient();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            //queryString查找,连接词AND必须大写
            String params = "itemid:cpu1";
            boolQueryBuilder.must(QueryBuilders.queryStringQuery(params));
            //范围查找
            boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").gt(1592664200).lt(1592666900));

            TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("_groupBy").field("");

            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.size(100);
            //请求ES连接
            SearchRequest searchRequest = new SearchRequest(index).source(searchSourceBuilder);
            //查询
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            elasticPoolUtil.returnClient(client);
        }
    }
}
