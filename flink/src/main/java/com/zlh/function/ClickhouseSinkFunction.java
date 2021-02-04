package com.zlh.function;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

/**
 * Clickhouse Sink
 * @package com.zlh.function
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/12/7
 */
@Slf4j
public class ClickhouseSinkFunction extends RichSinkFunction<String> implements Serializable {
    private String tableName;
    private String[] tableColums;
    private List<String> types;
    private String[] columns;
    private String userName;
    private String password;
    private String[] ips;
    private String driverName = "ru.yandex.clickhouse.ClickHouseDriver";
    private List<String> list = new ArrayList<>();
    private List<PreparedStatement> preparedStatementList = new ArrayList<>();
    private List<Connection> connectionList = new ArrayList<>();
    private List<Statement> statementList = new ArrayList<>();

    private long lastInsertTime = 0L;
    private long insertCkTimenterval = 4000L;
    /** 插入的批次 */
    private int insertCkBatchSize = 10000;

    /**
     * 必要参数：tableName、ips、tableColums
     * @param tableName
     * @param userName
     * @param password
     * @param ips
     * @param tableColums
     * @param types
     * @param columns
     */
    public ClickhouseSinkFunction(String tableName, String[] ips, String[] tableColums, List<String> types, String[] columns, String userName, String password) {
        this.tableName = tableName;
        this.userName = userName;
        this.password = password;
        this.ips = ips;
        this.tableColums = tableColums;
        this.types = types;
        // 新增字段
        this.columns = columns;
    }

    /**
     * 插入数据
     * @param rows
     * @param preparedStatement
     * @param connection
     * @throws SQLException
     */
    public void insertData(List<String> rows, PreparedStatement preparedStatement, Connection connection) throws SQLException {

        for (int i = 0; i < rows.size(); ++i) {
            String row = rows.get(i);
            JSONObject jsonObject = JSONObject.parseObject(row);
            for (int j = 0; j < this.tableColums.length; ++j) {
                String value = jsonObject.getString(tableColums[j]);
                if (null != value) {
                    preparedStatement.setObject(j + 1, value);
                } else {
                    preparedStatement.setObject(j + 1, "null");
                }
            }
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        connection.commit();
        preparedStatement.clearBatch();
    }


    /**
     * 新增字段修改表添加列
     *
     * @param statement
     * @throws Exception
     */
    public void tableAddColumn(Statement statement) {
        try {
            if (null != this.columns && this.columns.length > 0) {

                /**
                 * table 增加字段
                 */
                // 获取原表字段名
                String querySql = "select * from " + this.tableName + " limit 1";

                ResultSet rs = statement.executeQuery(querySql);
                ResultSetMetaData rss = rs.getMetaData();
                int columnCount = rss.getColumnCount();

                List<String> orgTabCols = new ArrayList<>();
                for (int i = 1; i <= columnCount; ++i) {
                    orgTabCols.add(rss.getColumnName(i));
                }

                // 对比两个数组,判断新增字段是否在原来的表中
                Collection collection = new ArrayList<>(orgTabCols);
                boolean exists = collection.removeAll(Arrays.asList(this.columns));

                // 新增字段不在原来的表中，执行添加列操作
                if (!exists) {

                    for (int i = 0; i < this.columns.length; ++i) {

                        StringBuilder sb = null;
                        StringBuilder sb_all = null;
                        if (i == 0) {
                            sb.append("alter table " ).append(this.tableName).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(orgTabCols.get(orgTabCols.size() - 1));
                            sb_all.append("alter table " ).append("_all").append(this.tableName).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(orgTabCols.get(orgTabCols.size() - 1));

                        } else {
                            sb.append("alter table " ).append(this.tableName).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(this.columns[i - 1]);

                            sb_all.append("alter table " ).append("_all").append(this.tableName).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(this.columns[i - 1]);
                        }

                        if (StringUtils.isNotEmpty(sb.toString())) {
                            statement.executeUpdate(sb.toString());
                        }

                        if (StringUtils.isNotEmpty(sb_all.toString())) {
                            statement.executeUpdate(sb_all.toString());
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("{}: ",e);
        }
    }

    /**
     * 根据IP创建连接
     * @throws Exception
     */
    public void createConnection() throws Exception {

        // 插入语句
        String insertStr = this.clickhouseInsertValue(this.tableColums, this.tableName);
        // 创建表
//        List<String> createtableStrList = StrUtils.clickhouseCreatTable(this.tableColums, this.tableName, Constant.CKCLUSTERNAME, this.tableColums[3], this.types);
        // 创建数据库
        String create_database_str = "create database if not exists " + this.tableName.split("\\.")[0];

        for (String ip : this.ips) {
            String url = "jdbc:clickhouse://" + ip + ":8123";
            Connection connection = DriverManager.getConnection(url, this.userName, this.password);
            Statement statement = connection.createStatement();

            // 执行创建数据库
            statement.executeUpdate(create_database_str);

            // 执行创建表
//            statement.executeUpdate(createtableStrList.get(0));
//            statement.executeUpdate(createtableStrList.get(1));

            // 增加表字段
            tableAddColumn(statement);

            this.statementList.add(statement);
            PreparedStatement preparedStatement = connection.prepareStatement(insertStr);
            connection.setAutoCommit(false);
            this.preparedStatementList.add(preparedStatement);
            this.connectionList.add(connection);
        }

    }

    /**
     * 组装insert预编译语句
     * @param tableColums
     * @param tableName
     * @return
     */
    private String clickhouseInsertValue(String[] tableColums, String tableName) {
        StringBuffer tables = new StringBuffer();
        tables.append("insert into "+tableName+"("+tableColums[0]);
        StringBuffer values = new StringBuffer();
        values.append(" values(?");
        for (int i=1;i<tableColums.length;i++){
            tables.append(",");
            tables.append(tableColums[i]);
            values.append(",?");
        }
        tables.append(")");
        values.append(")");
        tables.append(values);
        return tables.toString();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(this.driverName);
        // 创建连接
        createConnection();
    }

    @Override
    public void invoke(String row, Context context) throws Exception {
        // 轮询写入各个local表，避免单节点数据过多
        if (null != row) {
            Random random = new Random();
            int index = random.nextInt(this.ips.length);
            if(list.size() >= this.insertCkBatchSize || isTimeToDoInsert()) {
                insertData(list,preparedStatementList.get(index),connectionList.get(index));
                list.clear();
                this.lastInsertTime = System.currentTimeMillis();
            } else {
                list.add(row);
            }
        }
    }

    @Override
    public void close() throws Exception {

        for (Statement statement : this.statementList) {
            if (null != statement) {
                statement.close();
            }
        }

        for (PreparedStatement preparedStatement : this.preparedStatementList) {
            if (null != preparedStatement) {
                preparedStatement.close();
            }
        }

        for (Connection connection : this.connectionList) {
            if (null != connection) {
                connection.close();
            }
        }
    }

    /**
     * 根据时间判断是否插入数据
     *
     * @return
     */
    private boolean isTimeToDoInsert() {
        long currTime = System.currentTimeMillis();
        return currTime - this.lastInsertTime >= this.insertCkTimenterval;
    }
}
