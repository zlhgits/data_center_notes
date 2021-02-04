package com.zlh;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

/**
 * @package com.zlh
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/9/1
 */
public class FLinkSqlBatch {
    public static void main(String[] args) throws Exception {
        FLinkSqlBatch fLinkSqlBatch = new FLinkSqlBatch();
//        fLinkSqlBatch.sqlBatchTest1(args);
        fLinkSqlBatch.sqlBatchTest2(args);
    }
     private void sqlBatchTest1(String[] args) throws Exception {
         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
         // 2）读取数据
         DataSet<AdPojo> csvInput = env
                 .readCsvFile("/Users/zlh/test_file/23456.csv")
                 //设置要读取的属性列   1为读   0为不读
//                 .includeFields("10010")
                 .ignoreFirstLine() //忽略第一行
                 //itemid,timestamp,value,host,ip,systemName
                 .pojoType(AdPojo.class, "itemid", "timestamp", "value", "host", "ip", "systemName");
         csvInput.print();
         //3）将DataSet转换为Table，并注册为table1
         Table topScore = tableEnv.fromDataSet(csvInput);
         tableEnv.registerTable("table1", topScore);

         //4）自定义sql语句,如value这种关键字必须使用``
         Table groupedByCountry = tableEnv.sqlQuery("select itemid,`timestamp`,`value`,host,ip,systemName from table1");
         //5）转换回dataset,结果集必须是全部pojo
         DataSet<AdPojo> result = tableEnv.toDataSet(groupedByCountry, AdPojo.class);
         //6）打印
         result.print();
     }

    private void sqlBatchTest2(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        // 2）读取数据
        String[] fileds = {"itemid", "timestamp", "value", "host", "ip", "systemName"};
        TypeInformation<?>[] typeInformations = {Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING,Types.STRING};
        TableSource csvTableSource = new CsvTableSource("/Users/zlh/test_file/23456.csv", fileds, typeInformations);
        //3）将DataSet转换为Table，并注册为table1
        tableEnv.registerTableSource("csvTableSource",csvTableSource);
        //4）自定义sql语句,如value这种关键字必须使用``
        Table table = tableEnv.sqlQuery("select itemid,`timestamp`,`value`,host from csvTableSource");
        //将查询到的结果注册成表
        tableEnv.registerTable("inputTable",table);
        //5）结果集写出csv
        String[] outFileds = {"itemid", "timestamp", "value", "host"};
        TypeInformation<?>[] outTypeInformations = {Types.STRING,Types.STRING,Types.STRING,Types.STRING};
        //numFiles为1的时候第一个参数为file,大于1就为path，覆盖模式下文件路径可重复
        TableSink csvSink = new CsvTableSink("/Users/zlh/test_file/outFile/out_test2.csv",",",1, FileSystem.WriteMode.OVERWRITE)
                //在csvTableSink中设置，TableSink接口设置方法已过时
                .configure(outFileds,outTypeInformations);
        //注册sink表
        tableEnv.registerTableSink("outTable",csvSink);
        //将结果表写入TableSink
        tableEnv.sqlUpdate("insert into outTable select * from inputTable");

        //执行
        tableEnv.execute("csv input out full");
    }
}
