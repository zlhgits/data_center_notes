package com.zlh.elasticsearch;

import com.zlh.util.DateUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

/**
 * @package com.zlh.elasticsearch
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/7/15
 */
public class EsOprateUtil {
    private static Date statsTime = null;

    /**
     * 重试次数
     */
    private final static int RETRYCNT = 3;
    private static final int MONTH_VALUE = 10;
    private static final String FIELD_IOC_ID_S = "iocIds";
    private static final String FIELD_COLLIDE_TIME = "collideTime";
    private final static String ES_AGG_TERMS = "_terms";
    private final static String ES_AGG_SUM = "_sum";
    /**
     * iocID,sourceId,hitCount,collideTime
     */
    private final static String SUM_FIELD = "hitCount";
    private final static String TIME_FIELD = "collideTime";
    private final static String HIT_LAST_TIME = "lastTime";
    public static void main(String[] args) throws IOException {
        ElasticPoolUtil elasticPoolUtil = new ElasticPoolUtil();
        EsOprateUtil esOprateUtil = new EsOprateUtil();
        Date today = new Date();
        Date today30 = esOprateUtil.getDateAmountOfBefor(today,30);
        RestHighLevelClient client = null;
        try {
            client = elasticPoolUtil.getClient();
            Map<String, Integer> iocIdMap = esOprateUtil.getGroupSum(client,new String[]{"dc_ioc_hit_*"},"iocID", today30, today);
            System.out.println(iocIdMap);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            elasticPoolUtil.returnClient(client);
        }
    }
    public Map<String, Integer> getGroupSum(RestHighLevelClient client, String[] index, String groupField, Date startTime, Date endTime) throws Exception {
        Map<String,Integer> groupMap = new LinkedHashMap<>(32);
        try {
            //判断索引是否存在
            GetIndexRequest exist=new GetIndexRequest(index);
            boolean exists=client.indices().exists(exist, RequestOptions.DEFAULT);
            if (!exists){
                return groupMap;
            }
            SearchRequest searchRequest = new SearchRequest(index);

            //分组terms下的统计
            TermsAggregationBuilder groupBuilder= AggregationBuilders.terms(ES_AGG_TERMS).field(groupField);
            groupBuilder.subAggregation(AggregationBuilders.sum(ES_AGG_SUM).field(SUM_FIELD));
            groupBuilder.size(Integer.MAX_VALUE);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            //查询条件
            if (startTime != null && endTime != null){
                //前包后包
                sourceBuilder.query(QueryBuilders.rangeQuery(TIME_FIELD).gte(startTime.getTime()).lte(endTime.getTime()));
            }
            sourceBuilder.aggregation(groupBuilder);
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest,RequestOptions.DEFAULT);

            Aggregations sourceAggregations = response.getAggregations();
            //获取分组统计
            ParsedTerms parsedTerms = sourceAggregations.get(ES_AGG_TERMS);
            List<? extends Terms.Bucket> bucketList = parsedTerms.getBuckets();
            for (Terms.Bucket bucket : bucketList){
                String key = bucket.getKeyAsString();
                ParsedSum parsedValueSum = bucket.getAggregations().get(ES_AGG_SUM);
                Double sum = parsedValueSum.getValue();
                groupMap.put(key,sum.intValue());
            }

            return groupMap;
        }catch (Exception e){
            e.printStackTrace();
            throw new Exception("分组统计查询失败",e);
        }
    }

    /**
     * 近smount天0时
     * @param day
     * @param amount
     * @return
     */
    private Date getDateAmountOfBefor(Date day, int amount) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(day);
        cal.add(Calendar.DATE, -amount);
        String amountStr = DateUtil.dateToString("yyyy-MM-dd 00:00:00", cal.getTime());
        return  DateUtil.strToDate("",amountStr);
    }
}
