package com.maywide.dbt.core.services;

import com.maywide.dbt.config.datasource.dynamic.DbContextHolder;
import com.maywide.dbt.util.DateUtils;
import com.maywide.dbt.util.SpringJdbcTemplate;
import com.maywide.dbt.util.SqlUtil;
import com.maywide.dbt.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class JdbcUtilServices {

    private static Logger log = LoggerFactory.getLogger(JdbcUtilServices.class);
    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    //锁对象
    private static Object lock = new Object();


    /***
     * 批量插入数据库，同时插入一直备份表(备份表名= 表+昨天的日期(table_yymmdd))
     * @param inDataSource
     * @param targetTable
     * @param valueList
     * @return
     */
    @Transactional
    public boolean batchInsertAndBackYesTable(String inDataSource,String targetTable , List<Map<String,Object>> valueList){
        Date yesterday = org.apache.commons.lang.time.DateUtils.addDays(new Date(),-1);
        return this.batchInsertAndBack(inDataSource,targetTable,valueList,yesterday);
    }

    /***
     * 批量插入数据库，同时插入一直备份表(备份表名= 表+指定日期(table_yymmdd))
     * @param inDataSource
     * @param targetTable
     * @param valueList
     * @param day
     * @return
     */
    @Transactional
    public boolean batchInsertAndBack(String inDataSource,String targetTable , List<Map<String,Object>> valueList,Date day){
        String todayStr = DateUtils.formatDate(day,DateUtils.FORMAT_YYYYMMDD);
        String backTable = targetTable + "_" +todayStr;
        this.batchInsert(inDataSource,backTable,valueList);
        this.batchInsert(inDataSource,targetTable,valueList);
        return true;
    }
    @Transactional
    public int[] batchInsert(String inDataSource,String targetTable , List<Map<String,Object>> valueList){
        if(null == valueList || valueList.isEmpty()){
            log.info("准备插入数到+"+inDataSource+",table ="+targetTable+",数量为空,不执行插入");
            return null;
        }
        log.info("准备插入数到+"+inDataSource+",table ="+targetTable+",数量="+valueList.size());
        StringBuffer insertSql =new StringBuffer( "  insert into "+targetTable+" ( ");

        StringBuffer paramSb= new StringBuffer();
        Map<String,Object> oneMap = valueList.get(0);
        Object[] keys = oneMap.keySet().toArray();
        for (int i = 0; i < keys.length; i++) {
            if(i == 0 ){
                insertSql.append(keys[i]);
                paramSb.append("?");
            }else {
                insertSql.append(","+keys[i]);
                paramSb.append(",?");
            }
        }
        insertSql.append(") ").append(" values ( ").append(paramSb.toString()).append(" ) ");
        //System.out.println("SQL =" +insertSql);
        long t1 = System.currentTimeMillis();
        DbContextHolder.setDBType(inDataSource);
        int[] result =  springJdbcTemplate.batchUpdate(insertSql.toString(), new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement preparedStatement, int i) throws SQLException {
                Map<String,Object> oneMap = valueList.get(i);
                for (int j = 0; j < keys.length; j++) {
                    preparedStatement.setObject(j+1,oneMap.get(keys[j]));
                }
            }
            @Override
            public int getBatchSize() {
                return valueList.size();
            }
        });
        long t2 = System.currentTimeMillis();
        log.info("批量插入- [成功] - "+inDataSource+",table ="+targetTable+",数量="+valueList.size()+"耗时:["+(t2-t1)+"ms]");
        return result;
    }

    /***
     * 备份表，将指定的表全表备份，新的表名为 table_name_yyyymmdd
     * @param inDataSource
     * @param targetTable
     */
    public void backUpTable(String inDataSource, String targetTable) {
        String newTable = TableUtil.getYesterdayTableName(targetTable);
        if(this.validateTableNameExistByCon(inDataSource,newTable)){
            //删除表
            this.dropTable(inDataSource,newTable);
        }
        log.info("备份表 - 库["+inDataSource+"]的表:["+targetTable+"]到["+newTable+"]......开始");
        //备份数据
        String backUpSql = " create table "+ newTable +" as SELECT * FROM " + targetTable;
        DbContextHolder.setDBType(inDataSource);
        int x = springJdbcTemplate.update(backUpSql);
        log.info("备份表 - 库["+inDataSource+"]的表:["+targetTable+"]到["+newTable+"].......成功");
    }

    /***
     * 验证表存在否
     * @param datasource
     * @param table
     * @return
     */
    public boolean existTable(String datasource,String table){
        return this.validateTableNameExistByCon(datasource,table);
    }
    /***
     * 复制表结构
     * @param datasource
     * @param newTable
     * @param oldTable
     */
    public void copyTableNoData(String datasource,String oldTable,String newTable){
        synchronized (JdbcUtilServices.lock){
            if (!this.existTable(datasource,newTable)){
                DbContextHolder.setDBType(datasource);
                String copySql =" create table "+ newTable +" as SELECT * FROM " + oldTable +" where 1 = 2";
                int x = springJdbcTemplate.update(copySql);
                log.info(" 数据库["+datasource+"]的 【"+newTable+"】创建成功");
            }
        }
    }

    /***
     * 按日期复制表结构
     * @param datasource
     * @param oldTable
     */
    public void copyTableNoDataByYesterday(String datasource,String oldTable){
        this.copyTableNoData(datasource,oldTable,TableUtil.getYesterdayTableName(oldTable));
    }

    /***
     * 通过Connecion获取表是否存在
     * @param inDataSource 数据库名
     * @param tableName    表明
     * @return
     */
    public  boolean  validateTableNameExistByCon(String inDataSource,String tableName) {
        DbContextHolder.setDBType(inDataSource);
        boolean isExist = false;
        Connection con = null;
        ResultSet rs =null;
        try {
            con = this.springJdbcTemplate.getDataSource().getConnection();
            rs = con.getMetaData().getTables(null, null, tableName, null);
            if (rs.next()) {
                isExist =  true;
            } else {
                isExist =  false;
            }
        } catch (SQLException e) {
            log.info(e.getMessage(),e);
            e.printStackTrace();
        }finally {
            try {
                if(null != rs && !rs.isClosed()){
                    rs.close();
                }
                if(null != con && !con.isClosed()){
                    con.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                log.error(e.getMessage(),e);
            }

        }
        log.info("判断表存在 - 表 "+tableName +"在不在 数据库["+inDataSource+"]中 = 【"+isExist+"】");
        return isExist;
    }


    /***
     * 清除数据库的表数据
     * @param inDataSource
     * @param targetTable
     */
    public void clearTable(String inDataSource, String targetTable) {
        String delAllData = "delete from "+targetTable ;
        DbContextHolder.setDBType(inDataSource);
        int r = springJdbcTemplate.update(delAllData);
        log.info("清空数据 - 库["+inDataSource+"]中的表:[ " + targetTable+"]") ;
    }

    /***
     * 删除指定数据库的表
     * @param inDataSource
     * @param targetTable
     */
    public void dropTable(String inDataSource, String targetTable) {
        String dropTable = "drop table "+targetTable ;
        DbContextHolder.setDBType(inDataSource);
        springJdbcTemplate.execute(dropTable);
        log.info("删除表 - 库["+inDataSource+"]中的表:[ " + dropTable+"]") ;
    }

    /***
     * 解析SQL 获取数据
     * @param sourceDataSource
     * @param sourceSql
     * @return
     */
    public List<Map<String,Object>> getListBySql(String sourceDataSource,String sourceSql){
        long t1 = System.currentTimeMillis();
        DbContextHolder.setDBType(sourceDataSource);
        List<Map<String,Object>> list = springJdbcTemplate.queryForList(sourceSql);
        long t2 = System.currentTimeMillis();
        log.info("[查询SQL] - 耗时  ["+ (t2 - t1)+"]");
        return list;
    }

    /***
     *
     * @param sourceDataSource
     * @param sql
     * @return
     */
    public int count(String sourceDataSource,String sql){
        DbContextHolder.setDBType(sourceDataSource);
        return springJdbcTemplate.queryForObject(SqlUtil.countSql(sql), Integer.class);
    }



}
