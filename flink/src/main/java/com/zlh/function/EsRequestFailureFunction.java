package com.zlh.function;

import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.ExceptionUtils;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketTimeoutException;

/**
 * @package com.zlh.util
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
public class EsRequestFailureFunction implements ActionRequestFailureHandler {
    private static final Logger logger = LoggerFactory.getLogger(EsRequestFailureFunction.class);

    @Override
    public void onFailure(ActionRequest actionRequest, Throwable failure, int i, RequestIndexer requestIndexer) throws Throwable {
         if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
             // 异常1: ES队列满了(Reject异常)，放回队列
             //放入队列会卡住整个程序
      //       requestIndexer.add(actionRequest);
             logger.error("-----------------Elasticsearch EsRejectedExecutionException------------------");
             logger.error("EsRejectedExecutionException: {} ,exceptionStackTrace: {}",actionRequest.toString(),failure.fillInStackTrace());
         } else if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
             // 异常2: ES超时异常(timeout异常)，放回队列
            logger.error("-----------------Elasticsearch SocketTimeoutException------------------");
             logger.error("SocketTimeoutException: {} ,exceptionStackTrace: {}",actionRequest.toString(),failure.fillInStackTrace());
         } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
            // 异常3: ES语法异常，丢弃数据，记录日志
             logger.error("Sink to es exception1 ,exceptionData1: {} ,exceptionStackTrace: {}",actionRequest.toString(),failure.fillInStackTrace());
         }   else {
             logger.error("Sink to es exception2 ,exceptionData2: {} ,exceptionStackTrace: {}",actionRequest.toString(),failure.fillInStackTrace());
         }
    }
}
