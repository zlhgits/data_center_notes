package com.zlh.hsqldb;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @package com.zlh.hsqldb
 * @company: dacheng
 * @author: zlh
 * @createDate: 2020/8/19
 */
public class Demo1 {
    public static void main(String[] args) throws Exception {
        inProcessFile();
    }
    private static void inProcessFile() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:file:/Users/zlh/db/test_database;shutdown=true", "SA", "");
        Statement stmt=null;
        ResultSet rs=null;
        if (conn != null) {
            stmt = conn.createStatement();
            System.out.println("Connected db success!");

            //itemid int,timestamp int,value int,host varchar(255),ip varchar(255),systemName varchar(255)
            //文本表不需要严格的大小控制varchar(1)，普通表必须有足够的大小varchar(100),列名不区分大小写
            String sql = "drop table test1 IF EXISTS;drop table test2 IF EXISTS;" +
                    "CREATE Text TABLE test1(itemid varchar(1),TIMESTAMP varchar(255),value int,host varchar(255),ip varchar(255),systemName varchar(255));"+
                    "CREATE TABLE test2(ITEMID varchar(100),timestamp varchar(255),value varchar(255),host varchar(255),ip varchar(255),systemName varchar(255));";
            stmt.execute(sql);

            sql ="SET TABLE test1 SOURCE '0810/98765.csv;fs=,;ignore_first=true;encoding=UTF-8'";
            stmt.execute(sql);

            //复杂列转行,distinct可去重
            sql = "insert into test2 select ITEMID,GROUP_CONCAT(distinct timestamp) as timestamp,GROUP_CONCAT(value) as value,GROUP_CONCAT(host) as host," +
                    "GROUP_CONCAT(ip) as ip,GROUP_CONCAT(distinct systemName) as systemName from test1 group by itemid;" +
                    "insert into test2 values('8658765','8658765','8658765','8658765','8658765','8658765')";
            stmt.execute(sql);

            //join
            sql = "select t1.itemid,t2.systemName,t2.timestamp from test2 t2 join test1 t1 on t1.itemid=t2.itemid";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("itemid="+rs.getString("itemid"));
                System.out.println("systemName="+rs.getString("systemName"));
                System.out.println("timestamp="+rs.getString("timestamp"));
            }

            //group
            sql ="select concat(itemid,'-',systemName,'.csv') as s_concat,3000/case when sum(value)=0 then 1 else sum(value) end as sum_v from test1 group by itemid,systemName";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("分组合并字段s_concat="+rs.getString("s_concat"));
                System.out.println("分组sum字段sum_v="+rs.getString("sum_v"));
            }

            //date 没有解析AM/PM的功能,自定义函数解决
            sql ="drop FUNCTION dateHandle IF EXISTS;CREATE FUNCTION dateHandle(v VARCHAR(50)) RETURNS VARCHAR(50)\n" +
                    "   LANGUAGE JAVA DETERMINISTIC NO SQL\n" +
                    "   EXTERNAL NAME 'CLASSPATH:com.zlh.hsqldb.Demo1.dateHandle'";
            stmt.execute(sql);

            sql = "select dateHandle('10-AUG-20 11.07.00.000000 AM') as dt from test1";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("时间日期="+rs.getString("dt"));
            }

            sql ="select count(*) as s_count from test2";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("s_count="+rs.getString("s_count"));
            }
            if (stmt != null) {
                stmt.close();
            }
            conn.close();
        }
    }
    private static void inProcessMem() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:hsqldb:file:/Users/zlh/db/test_database;shutdown=true", "SA", "");
//        Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb;shutdown=true", "SA", "");
        Statement stmt=null;
        ResultSet rs=null;
        if (conn != null) {
            stmt = conn.createStatement();
            System.out.println("Connected db success!");
            //file模式已创建的表不能再次创建
            String sql = "CREATE TABLE test(ID int, NAME varchar(255),);";
            stmt.execute(sql);

            sql = "INSERT INTO test(ID, NAME) VALUES ('1', 'ADMIN');";
            stmt.executeUpdate(sql);
            sql = "INSERT INTO test(ID, NAME) VALUES ('2', 'operate');";
            stmt.executeUpdate(sql);

            sql ="select * from test";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("id="+rs.getString("id"));
                System.out.println("name="+rs.getString("name"));
            }

            sql ="select count(*) as s_count from test";
            rs=stmt.executeQuery( sql);
            while(rs.next() )
            {
                System.out.println("s_count="+rs.getString("s_count"));
            }
            if (stmt != null) {
                stmt.close();
            }
            conn.close();
        }
    }

    /**
     * 测试自定义函数
     * @return
     * @throws SQLException
     */
    public static String dateHandle(String date) throws ParseException {
        String pattern = "dd-MMM-yy hh.mm.ss.SSSSSS a";
        String pattern1 = "yyyy-MM-dd HH:mm:ss";
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.ENGLISH);
        SimpleDateFormat sdf1 = new SimpleDateFormat(pattern1);
        Date parse = sdf.parse(date);
        String format = sdf1.format(parse);
        System.out.println(format);
        return format;
    }
}
