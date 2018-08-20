package com.maywide.dbt.core.execute;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.maywide.dbt.config.datasource.dynamic.Constants;
import com.maywide.dbt.config.datasource.dynamic.DbContextHolder;
import com.maywide.dbt.core.pojo.oracle.TableInfo;
import com.maywide.dbt.core.services.JdbcUtilServices;
import com.maywide.dbt.util.SpringJdbcTemplate;
import com.maywide.dbt.util.SqlUtil;
import org.apache.tomcat.util.threads.ThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class DataTransport {

    private static final Logger log = LoggerFactory.getLogger(DataTransport.class);
    public static final int WORK_QUE_SIZE = 3000;
    public static final int BATCH_PAGESIZE = 5000;

    public static ConcurrentHashMap<String,JSONObject> successMap = new ConcurrentHashMap<>();

    private static  final AtomicLong along = new AtomicLong(0);

//    public static ThreadPoolExecutor dataCopyPoolExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2 + 1, Runtime.getRuntime().availableProcessors() * 3 + 1, 30, TimeUnit.SECONDS,
//            new LinkedBlockingDeque<>(DataTransport.WORK_QUE_SIZE),new ThreadPoolExecutor.CallerRunsPolicy());

    public static ThreadPoolExecutor dataCopyPoolExecutor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 10 + 1, Runtime.getRuntime().availableProcessors() * 15+ 1, 30, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(DataTransport.WORK_QUE_SIZE),new ThreadPoolExecutor.CallerRunsPolicy());

    @Autowired
    private SpringJdbcTemplate springJdbcTemplate ;

    @Autowired
    private JdbcUtilServices jdbcUtilServices;

    @Autowired
    private TableTransport tableTransport;


    public void startCopyData(String oriDatasource,String schema,List<String> targetDBlist,String tableName ){
        if(null == targetDBlist || targetDBlist.isEmpty()){
            log.error("targetDBlist 为空 ");
            return ;
        }
        successMap = new ConcurrentHashMap<>();

        new Thread(new CopyTableDataWork(oriDatasource,schema,targetDBlist,tableName)).start();
    }

    //1.获取oracle表 和数据
    //2.针对表进行mysql的数据插入
    //3.多线程执行(datasource,tableName)
    private class CopyTableDataWork implements Runnable {
        private AtomicInteger  ai  = new AtomicInteger(0);
        private long all_data = 0;
//        private long startTime = System.currentTimeMillis();
        private String oriDatasource;
        private String schema;
        //private String targetDatasource;
        private String tableName;

        private List<String> targetDBlist ;

        public CopyTableDataWork(String oriDatasource,String schema, List<String>  targetDBlist, String tableName) {
            this.oriDatasource = oriDatasource;
            this.schema = schema;
            this.targetDBlist = targetDBlist;
            this.tableName = tableName;
        }

        @Override
        public void run() {
            try {
                //1.判断表在否（不在的话,就调用tableTransprort 复制表）
                for (String targetDb : targetDBlist) {
                    TableInfo tableInfo = null;
                    if(!jdbcUtilServices.existTable(targetDb,tableName)){
                        if(null == tableInfo){
                            tableInfo = tableTransport.initTable(oriDatasource,schema,tableName);
                        }
                        tableTransport.copyTableToMySql(targetDb,tableInfo);
                    }
                }
                //2.查询源数据
                String sourceSql = "select * from "+schema+"."+tableName;
                int count = jdbcUtilServices.count(oriDatasource,sourceSql);
                log.info("统计出 数据库【"+oriDatasource+"】的数据总数【"+count+"】,即将插入目标数据库【"+JSON.toJSONString(targetDBlist)+"】的表【"+tableName+"】");

                int pageSize =DataTransport.BATCH_PAGESIZE;
                all_data = count;
                //3.复制数据到指定表（多线程)
                if(count > pageSize){
                    int totalPageNum = (count  +  pageSize  - 1) / pageSize;
                    log.error("统计出 数据库【"+oriDatasource+"】的数据总数【"+count+"】,每页["+pageSize+"],总共["+totalPageNum+"]页,即将插入目标数据库【"+JSON.toJSONString(targetDBlist)+"】的表【"+tableName+"】");
                    int start = 0;
                    int end = 0;
                    for (int i = 0; i < totalPageNum; i++) {
                        log.debug("数据="+(i+1)*pageSize);
                        start = (i)*pageSize;
                        // end = (i+1)*pageSize;
                        // mysql 的分页 String sql = sourceSql +" limit " + i * pageSize + "," + 1 * pageSize;
                        // oracle 的分页
                        String dbProductName =tableTransport.getDbName(oriDatasource);
                        String sql = SqlUtil.pageSql(dbProductName,sourceSql,start,pageSize);
                        log.debug("分页 sql : "+ sql);
                        dataCopyPoolExecutor.execute(new CopyDataInWork(sql));
                    }
                }else {
                    log.error("统计出 数据库【"+oriDatasource+"】的数据总数【"+count+"】,每页["+pageSize+"],总共[1]页,即将插入目标数据库【"+JSON.toJSONString(targetDBlist)+"】的表【"+tableName+"】");

                    dataCopyPoolExecutor.execute(new CopyDataInWork(sourceSql));
                }
            }catch (SQLException e){
                e.getErrorCode();
                log.error("复制数据时出错,oriDatasource="+oriDatasource+",schema="+schema+",targetDatasource="+JSON.toJSONString(targetDBlist)+",tableName"+tableName+","+e.getMessage());
            }

        }




        /***
         * 具体执行的线程类
         */
        private class CopyDataInWork implements Runnable {
            private String findSql ;

            private CopyDataInWork(String findSql) {
                this.findSql = findSql;
            }

            @Override
            public void run() {

                //log.info("线程 {"+Thread.currentThread().getName()+"},"+ JSON.toJSONString(DataTransport.dataCopyPoolExecutor));
                //1.根据源库和findSql  查询数据
                DbContextHolder.setDBType(oriDatasource);
                List<Map<String, Object>> valueList = springJdbcTemplate.queryForList(findSql);
                //2.根据数据，插入目标库
                // String logSql = "SQL："+findSql+"数据量:"+valueList.size();
                if(valueList.size() > 0){
                    //TODO 可以增加一个oracle 判断 去掉 rownum 字段，或者优化分页Sql
                    //logSql = logSql + JSON.toJSONString(valueList.get(0));
                    for (Map<String, Object> stringObjectMap : valueList) {
                        if(stringObjectMap.containsKey(Constants.ORACLE_PAGE_RONUM_FILED)){
                            stringObjectMap.remove(Constants.ORACLE_PAGE_RONUM_FILED);
                        }
                    }
                }

                // TODO 单个目标库 jdbcUtilServices.batchInsert(targetDatasource,tableName,valueList);
                int nowSuc = ai.addAndGet(valueList.size());
                for (String targetDb : targetDBlist) {
                    long startTime = System.currentTimeMillis();
                    jdbcUtilServices.batchInsert(targetDb,tableName,valueList);
                    log.info("表{"+tableName+"} 从 库{"+ oriDatasource +"} 到 目标库{" +targetDb+"} 目标的成功数为 "+nowSuc);
                    log.debug("写统计信息---开始");
                    JSONObject oneResult = new JSONObject();
                    oneResult.put("spend_time",(System.currentTimeMillis() - startTime)/1000);
                    oneResult.put("deal_count",nowSuc);
                    successMap.put(targetDb+"."+tableName,oneResult);
                    log.debug("写统计信息---完成");
                }
            }
        }
    }
}
