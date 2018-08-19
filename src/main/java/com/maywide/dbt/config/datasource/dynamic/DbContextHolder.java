package com.maywide.dbt.config.datasource.dynamic;

public class DbContextHolder {

    private static final ThreadLocal<String> contextHolder = new ThreadLocal<String>();

    public static void setDBType(String dbType) {
        contextHolder.set(dbType);
    }

    public static String getDBType() {
        return (String) contextHolder.get();
    }

    public static void clearDBType() {
        contextHolder.remove();
    }

    /**
     *  切换数据源语句     字符串为实体类GsoftDataSource中的name属性也就是数据库表BI_DATA_SOURCE中的c_name字段
     *  DbContextHolder.setDBType("dataSou7rceName");
     */
}