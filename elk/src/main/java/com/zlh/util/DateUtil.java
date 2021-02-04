package com.zlh.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期工具类, 继承org.apache.commons.lang.time.DateUtils类
 * @author xl
 *
 */
public class DateUtil extends DateUtils {


    private static String[] parsePatterns = {
            "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM",
            "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm", "yyyy/MM",
            "yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyy.MM.dd HH:mm", "yyyy.MM"};

    /**
     * 得到当前日期字符串 格式（yyyy-MM-dd）
     */
    public static String getDate() {
        return getDate("yyyy-MM-dd");
    }

    /**
     * 得到当前日期字符串 格式（yyyy-MM-dd） pattern可以为："yyyy-MM-dd" "HH:mm:ss" "E"
     */
    public static String getDate(String pattern) {
        return DateFormatUtils.format(new Date(), pattern);
    }

    /**
     * 得到日期字符串 默认格式（yyyy-MM-dd） pattern可以为："yyyy-MM-dd" "HH:mm:ss" "E"
     */
    public static String formatDate(Date date, Object... pattern) {
        String formatDate = null;
        if (pattern != null && pattern.length > 0) {
            formatDate = DateFormatUtils.format(date, pattern[0].toString());
        } else {
            formatDate = DateFormatUtils.format(date, "yyyy-MM-dd");
        }
        return formatDate;
    }

    /**
     * 得到日期时间字符串，转换格式（yyyy-MM-dd HH:mm:ss）
     */
    public static String formatDateTime(Date date) {
        return formatDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 得到当前时间字符串 格式（HH:mm:ss）
     */
    public static String getTime() {
        return formatDate(new Date(), "HH:mm:ss");
    }

    /**
     * 得到当前日期和时间字符串 格式（yyyy-MM-dd HH:mm:ss）
     */
    public static String getDateTime() {
        return formatDate(new Date(), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 得到当前年份字符串 格式（yyyy）
     */
    public static String getYear(Date date) {
        if (date == null) {
            return formatDate(new Date(), "yyyy");
        }
        return formatDate(date, "yyyy");
    }

    /**
     * 得到当前月份字符串 格式（MM）
     */
    public static String getMonth(Date date) {
        if (date == null) {
            return formatDate(new Date(), "MM");
        }
        return formatDate(date, "MM");
    }

    /**
     * 得到当天字符串 格式（dd）
     */
    public static String getDay(Date date) {
        if (date == null) {
            return formatDate(new Date(), "dd");
        }
        return formatDate(date, "dd");
    }

    /**
     * 得到当前星期字符串 格式（E）星期几
     */
    public static String getWeek(Date date) {
        if (date == null) {
            return formatDate(new Date(), "E");
        }
        return formatDate(date, "E");
    }

    /**
     * 日期型字符串转化为日期 格式
     * { "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm",
     *   "yyyy/MM/dd", "yyyy/MM/dd HH:mm:ss", "yyyy/MM/dd HH:mm",
     *   "yyyy.MM.dd", "yyyy.MM.dd HH:mm:ss", "yyyy.MM.dd HH:mm" }
     */
    public static Date parseDate(Object str) {
        if (str == null){
            return null;
        }
        try {
            return parseDate(str.toString(), parsePatterns);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * 计算工作时间年
     */
    public static int pastYears(Date workTime){
        long date = pastDays(workTime);
        int work = (int) (date / 365);
        return work;
    }

    /**
     * 获取过去的天数
     * @param date
     * @return
     */
    public static long pastDays(Date date) {
        long t = System.currentTimeMillis()-date.getTime();
        return t/(24*60*60*1000);
    }

    /**
     * 获取过去的小时
     * @param date
     * @return
     */
    public static long pastHour(Date date) {
        long t = System.currentTimeMillis()-date.getTime();
        return t/(60*60*1000);
    }

    /**
     * 获取过去的分钟
     * @param date
     * @return
     */
    public static long pastMinutes(Date date) {
        long t = System.currentTimeMillis()-date.getTime();
        return t/(60*1000);
    }

    /**
     * 转换为时间（天,时:分:秒.毫秒）
     * @param timeMillis
     * @return
     */
    public static String formatDateTime(long timeMillis){
        long day = timeMillis/(24*60*60*1000);
        long hour = (timeMillis/(60*60*1000)-day*24);
        long min = ((timeMillis/(60*1000))-day*24*60-hour*60);
        long s = (timeMillis/1000-day*24*60*60-hour*60*60-min*60);
        long sss = (timeMillis-day*24*60*60*1000-hour*60*60*1000-min*60*1000-s*1000);
        return (day>0?day+",":"")+hour+":"+min+":"+s+"."+sss;
    }

    /**
     * 获取两个日期之间的天数
     *
     * @param before
     * @param after
     * @return
     */
    public static double getDistanceOfTwoDate(Date before, Date after) {
        long beforeTime = before.getTime();
        long afterTime = after.getTime();
        BigDecimal decimal = BigDecimal.valueOf(afterTime - beforeTime).divide(BigDecimal.valueOf(1000 * 60 * 60 * 24));
        return decimal.doubleValue();
    }

    /**
     * String转date
     *
     * 得到当前日期字符串 格式（yyyy-MM-dd） pattern可以为："yyyy-MM-dd" "HH:mm:ss" "E"
     *
     */
    public static Date strToDate(String pattern,String source){
        Date date = new Date();
        try {
            if (pattern == null || "".equals(pattern) ) {
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                date = df.parse(source);
                return date ;
            }
            DateFormat df = new SimpleDateFormat(pattern);
            date = df.parse(source);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * Long转 date
     */
    public static Date longToDate(long date){
        Date d = new Date(date);
        return d;
    }

    /**
     * date转String
     */
    public static String dateToString(String pattern,Date date){
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * date 转long
     */
    public static long dateToLong(Date date){
        return date.getTime();
    }

    /**
     * 获取每月的第一天
     * @param date
     * @return
     */
    public static Date getMonthFirstDay(Date date){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar= Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
        String first = df.format(calendar.getTime());
        return calendar.getTime();
    }

    /**
     * 获取每月的最后一天
     * @param date
     * @return
     */
    public static Date getMonthLastDay(Date date){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar= Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        String last = df.format(calendar.getTime());
        return calendar.getTime();
    }
    /**
     * 判断是不是周六日
     * @param date
     * @return
     * @throws ParseException
     */
    public static boolean isWeek(Date date) throws ParseException {
        boolean flag = false;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if(calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY ||
                calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY){
            flag = true;
            return flag;
        }
        return flag;

    }
    /**
     * 包含多少个节假日
     * @param begin
     * @param end
     * @return
     */
    public static int getHoliday(Date begin,Date end){

        return 0;
    }

    /**
     * 俩个日期之间包含多少个周末
     * @throws ParseException
     * type true 双休
     * false 单休
     */

    public static int getWeekdayCount(Date begin,Date end,boolean type) throws ParseException{
        int count = 0;
        Date flag = begin;
        Calendar calendar = Calendar.getInstance();
        while (flag.compareTo(end) > -1) {
            calendar.setTime(flag);
            int week = calendar.get(Calendar.DAY_OF_WEEK) - 1;		    //判断是否为周六日
            if (type) {
                if(week == 0 || week == 6){//0为周日，6为周六
                    calendar.add(Calendar.DAY_OF_MONTH, +1);//跳出循环进入下一个日期
                    flag = calendar.getTime();
                    count ++;
                    continue;
                }
            }else{
                if(week == 0){
                    calendar.add(Calendar.DAY_OF_MONTH, +1);
                    flag = calendar.getTime();
                    count ++;
                    continue;
                }
            }
            calendar.add(Calendar.DAY_OF_MONTH, +1);
            flag = calendar.getTime();
        }
        return count;
    }


    /**
     * 获取当前时间的前N小时
     * 
     * @return
     */
    public static String getBeforeByHourTime(int ihour){
        String returnstr = "";
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - ihour);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        returnstr = df.format(calendar.getTime());
        return returnstr;
    }
}