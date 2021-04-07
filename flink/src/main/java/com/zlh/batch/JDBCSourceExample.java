package com.zlh.batch;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * @package com.zlh.batch
 * @company: dacheng
 * @author: zlh
 * @createDate: 2021/4/7
 */
@Slf4j
public class JDBCSourceExample {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Row> dbDataSet = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://192.168.50.100:13306/dc_aiops")
                        .setUsername("root")
                        .setPassword("234*(sdlj12")
                        .setQuery("select item_name,count(id) from dc_zw_metrics_describe_regex group by item_name")
                        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO,BasicTypeInfo.LONG_TYPE_INFO))
                        .finish()
        ).setParallelism(1);
        // 展开
        MapOperator<Row, Tuple2<String, Long>> operator = dbDataSet.map(new MapFunction<Row, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Row value) throws Exception {
                String itemName = value.getField(0).toString();
                Long count = Long.valueOf(value.getField(1).toString());
                return new Tuple2<>(itemName, count);
            }
        }).setParallelism(1);
        // 打印
        operator.print();
        // 写出到csv,并发度为1则会写入JDBCSourceExample，否则会创建JDBCSourceExample文件夹，文件夹下再创建parallelism个文件
        operator.writeAsCsv("file:///Users/zlh/JDBCSourceExample").setParallelism(1);
        // 写出到mysql,先转成Row格式
        operator.map(new MapFunction<Tuple2<String, Long>, Row>() {
            @Override
            public Row map(Tuple2<String, Long> value) throws Exception {
                Row row = new Row(2);
                row.setField(0,value.f0);
                row.setField(1,value.f1+10);
                return row;
            }
        }).output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                                .setDrivername("com.mysql.jdbc.Driver")
                                .setDBUrl("jdbc:mysql://192.168.50.98:13306/dczw")
                                .setUsername("root")
                                .setPassword("234*(sdlj12")
                                .setQuery("insert into test_flink_batch_sink(item_name,item_count) values(?,?)")
                                .finish()
        );
        env.execute("JDBC Source Example");
    }
}
