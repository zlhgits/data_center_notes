package com.zlh.batch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * @package com.zlh.batch
 * @company: dacheng
 * @author: zlh
 * @createDate: 2021/4/7
 */
@Slf4j
public class TextSourceWordCount {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text = env.fromElements(
                "Who is there?",
                "I think I hear them. Stand, ho! Who's there?");

        DataSet<Tuple3<String,String, Integer>> wordCounts = text
                .flatMap(new FlatMapFunction<String, Tuple3<String,String, Integer>>(){
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String,String, Integer>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(new Tuple3<String,String, Integer>(word,value, 1));
                        }
                    }
                })
                // 使用第1个字段分组，第3个字段相加
                .groupBy(0)
                .sum(2);

        wordCounts.print();
    }
}
