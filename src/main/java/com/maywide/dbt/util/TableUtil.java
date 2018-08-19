package com.maywide.dbt.util;

import java.util.Date;

public class TableUtil {

    /***
     * 将表名增加日期 rpt_sku - > rpt_yymmdd
     * @param tableName
     * @param day
     * @return
     */
    public static String getDayTableName(String tableName,Date day) {
        return tableName + "_" + DateUtils.formatDate(day, DateUtils.FORMAT_YYYYMMDD);
    }

    /****
     * 将表名增加昨天的日期
     * 如 rpt_sku - > rpt_sku_20180804
     * @return
     */
    public static String getYesterdayTableName(String tableName) {
        return TableUtil.getDayTableName(tableName,DateUtils.getYesterday());
    }
}
