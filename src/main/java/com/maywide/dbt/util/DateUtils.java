package com.maywide.dbt.util;

import java.text.*;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

	
	
    public final static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public final static String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";

    public final static DateFormat DEFAULT_TIME_FORMATER = new SimpleDateFormat(DEFAULT_TIME_FORMAT);

    public final static DateFormat DEFAULT_DATE_FORMATER = new SimpleDateFormat(DEFAULT_DATE_FORMAT);

    public final static String FORMAT_YYYY = "yyyy";

    public final static String FORMAT_YYYYMM = "yyyyMM";

    public final static String FORMAT_YYMMDD = "yyMMdd";

    public final static String FORMAT_YYYYMMDD = "yyyyMMdd";
    
    public final static String FORMAT_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    public final static DateFormat FORMAT_YYYY_FORMATER = new SimpleDateFormat(FORMAT_YYYY);

    public final static DateFormat FORMAT_YYYYMM_FORMATER = new SimpleDateFormat(FORMAT_YYYYMM);

    public final static DateFormat FORMAT_YYYYMMDD_FORMATER = new SimpleDateFormat(FORMAT_YYYYMMDD);
    
    public final static DateFormat FORMAT_YYYYMMDDHHMMSS_FORMATER = new SimpleDateFormat(FORMAT_YYYYMMDDHHMMSS);

    public static String formatDate(Date date) {
        if (date == null) {
            return null;
        }
        return DEFAULT_DATE_FORMATER.format(date);
    }

    public static String formatDate(Date date, String format) {
        if (date == null) {
            return null;
        }
        return new SimpleDateFormat(format).format(date);
    }

    public static String formatTime(Date date) {
        if (date == null) {
            return null;
        }
        return DEFAULT_TIME_FORMATER.format(date);
    }

    public static String formatDateNow() {
        return formatDate(new Date());
    }

    public static String formatTimeNow() {
        return formatTime(new Date());
    }

    public static Date parseDate(String date, DateFormat df) {
        if (date == null) {
            return null;
        }
        try {
            return df.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Date parseTime(String date, DateFormat df) {
        if (date == null) {
            return null;
        }
        try {
            return df.parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static Date parseDate(String date) {
        return parseDate(date, DEFAULT_DATE_FORMATER);
    }

    public static Date parseTime(String date) {
        return parseTime(date, DEFAULT_TIME_FORMATER);
    }

   /* public static String plusOneDay(String date) {
        DateTime dateTime = new DateTime(parseDate(date).getTime());
        return formatDate(dateTime.plusDays(1).toDate());
    }

    public static String plusOneDay(Date date) {
        DateTime dateTime = new DateTime(date.getTime());
        return formatDate(dateTime.plusDays(1).toDate());
    }*/

    public static String getHumanDisplayForTimediff(Long diffMillis) {
        if (diffMillis == null) {
            return "";
        }
        long day = diffMillis / (24 * 60 * 60 * 1000);
        long hour = (diffMillis / (60 * 60 * 1000) - day * 24);
        long min = ((diffMillis / (60 * 1000)) - day * 24 * 60 - hour * 60);
        long se = (diffMillis / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
        StringBuilder sb = new StringBuilder();
        if (day > 0) {
            sb.append(day + "D");
        }
        DecimalFormat df = new DecimalFormat("00");
        sb.append(df.format(hour) + ":");
        sb.append(df.format(min) + ":");
        sb.append(df.format(se));
        return sb.toString();
    }

    /**
     * 把类似2014-01-01 ~ 2014-01-30格式的单一字符串转换为两个元素数组
     */
    /*public static String[] parseBetweenDates(String date) {
        if (StringUtils.isBlank(date)) {
            return null;
        }
        String[] values = date.split("～");
        values[0] = values[0].trim();
        values[1] = values[1].trim();
        return values;
    }*/
    
    public static String getFormatDateString(Date date, String pattern){
    	if (FORMAT_YYYYMM.equals(pattern)) {
    		return FORMAT_YYYYMM_FORMATER.format(date);
    	} else if (FORMAT_YYYYMMDD.equals(pattern)) {
    		return FORMAT_YYYYMMDD_FORMATER.format(date);
    	} else if (FORMAT_YYYY.equals(pattern)) {
    		return FORMAT_YYYY_FORMATER.format(date);
    	} else if (DEFAULT_DATE_FORMAT.equals(pattern)) {
    		return DEFAULT_DATE_FORMATER.format(date);
    	} else if (DEFAULT_TIME_FORMAT.equals(pattern)) {
    		return DEFAULT_TIME_FORMATER.format(date);
    	} else {
    		return DEFAULT_TIME_FORMATER.format(date);
    	}
    }
    
    /**
     * 判断两个日期是否为同一天
     * @param data1
     * @param date2
     * @return
     */
    public static boolean isSameDay(Date data1, Date date2){
    	Calendar calendar1 = Calendar.getInstance();
    	calendar1.setTime(data1);
    	
    	Calendar calendar2 = Calendar.getInstance();
    	calendar2.setTime(date2);
    	
    	return (calendar1.get(Calendar.YEAR) == calendar2.get(Calendar.YEAR)) &&
    			(calendar1.get(Calendar.MONTH) == calendar2.get(Calendar.MONTH)) &&
    			(calendar1.get(Calendar.DAY_OF_MONTH) == calendar2.get(Calendar.DAY_OF_MONTH));
    }
    
    
    /**
     * 判断时间是否在某个月份内
     * @param date
     * @param current 0为当前,正值为接下来的，负值为之前的月份
     * @return
     */
    public static boolean isInSomeMonth(Date date, int current){
    	Calendar firstC= Calendar.getInstance();
    	firstC.add(Calendar.MONTH, current);
    	firstC.set(Calendar.DAY_OF_MONTH,1); //为选定的月份的第一天
    	Date first = firstC.getTime();
    	
    	Calendar lastC = Calendar.getInstance();
    	lastC.add(Calendar.MONTH, current);
    	lastC.set(Calendar.DAY_OF_MONTH, lastC.getActualMaximum(Calendar.DAY_OF_MONTH));
    	Date last = lastC.getTime();
    	
    	return date.after(first)&&date.before(last);
    }
    
    
	/**
	 * 将长时间格式字符串转换为时间 yyyy-MM-dd HH:mm:ss
	 * 
	 * @param strDate
	 * @return
	 */
	public static Date strToDateLong(String strDate) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		ParsePosition pos = new ParsePosition(0);
		Date strtodate = formatter.parse(strDate, pos);
		return strtodate;
	}

    /***
     * 获取昨天
     * @return
     */
	public static Date getYesterday(){
        return org.apache.commons.lang.time.DateUtils.addDays(new Date(),-1);
    }

    /***
     * 返回两个日期的相差天数
     * @param dateStart
     * @param dateEnd
     * @return
     */
    public static int getDiscrepantDays(Date dateStart ,Date dateEnd){
        return new Long((dateEnd.getTime() - dateStart.getTime()) / 1000 / 60
                / 60 / 24).intValue();
    }

    /**
     * 根据给出的日期，计算n天后的日期,n可以为负整数，表示n天前
     *
     * @param givenDate
     *            给定日期
     * @param n
     *            偏移量
     * @return n天后的日期
     */
    public static Date addNday(Date givenDate, int n) {
        Calendar c = Calendar.getInstance();
        c.setTime(givenDate);
        c.add(Calendar.DATE, n);
        return c.getTime();
    }
}
