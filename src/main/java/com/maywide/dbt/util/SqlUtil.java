package com.maywide.dbt.util;

import com.maywide.dbt.config.datasource.dynamic.Constants;

public class SqlUtil {

    public static  final String ORACLE = "Oracle";
    public static  final String MYSQL = "Mysql";
    /***
     * 获取统计Sql
     * 默认把第一个form的select 内容改为 count(*)
     * @param sql 复杂SQL  应该都改为 select * from ( 原sql )
     * @return
     */
    public static String countSql(String sql)  {
        String countSql = "";
        if (sql.toUpperCase().indexOf("UNION") > 0
                || sql.toUpperCase().indexOf("INTERSECT") > 0) {
            countSql = "SELECT COUNT(*) FROM (" + sql + ")";
        } else {
            countSql = "SELECT COUNT(*) FROM "
                    + sql.substring(sql.toUpperCase().indexOf("FROM") + 4);
        }
        return countSql;
    }

    /***
     * 根据数据库名称来获取分页Sql
     * @param dbProductName
     * @param querySql
     * @param startIndex
     * @param pageSize
     * @return
     */
    public static String pageSql(String dbProductName,String querySql,Integer startIndex, Integer pageSize){
        switch (dbProductName){
            case SqlUtil.ORACLE: return createOraclePageSQL(querySql,startIndex,startIndex+pageSize);
            case SqlUtil.MYSQL:return createMySQLPageSQL(querySql,startIndex,pageSize);
            default:return null;
        }
    }

    /**
     * 构造MySQL数据分页SQL
     * @param queryString
     * @param startIndex
     * @param pageSize
     * @return
     */
    public static String createMySQLPageSQL(String queryString, Integer startIndex, Integer pageSize) {
        String result = "";
        if (null != startIndex && null != pageSize) {
            result = queryString + " limit " + startIndex + "," + pageSize;
        } else if (null != startIndex && null == pageSize) {
            result = queryString + " limit " + startIndex;
        } else {
            result = queryString;
        }
        return result;
    }

    /**
     * 构造Oracle数据分页SQL
     * @param queryString
     * @param startIndex
     * @param lastIndex
     * @return
     */
    public static String createOraclePageSQL(String queryString, Integer startIndex, Integer lastIndex) {
        StringBuffer paginationSQL = new StringBuffer(" SELECT * FROM ( ");
        paginationSQL.append(" SELECT temp.* ,ROWNUM " + Constants.ORACLE_PAGE_RONUM_FILED+" FROM ( ");
        paginationSQL.append(queryString);
        paginationSQL.append(" ) temp where ROWNUM <= " + lastIndex);
        paginationSQL.append(" ) WHERE " + Constants.ORACLE_PAGE_RONUM_FILED+ " > " + startIndex);
        return paginationSQL.toString();
    }


}
