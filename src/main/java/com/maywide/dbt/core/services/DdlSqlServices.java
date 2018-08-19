package com.maywide.dbt.core.services;

import com.alibaba.fastjson.JSON;
import com.maywide.dbt.config.datasource.dynamic.DbContextHolder;
import com.maywide.dbt.core.pojo.oracle.*;
import com.maywide.dbt.util.SpringJdbcTemplate;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

@Component
public class DdlSqlServices {

    private static final Logger log = LoggerFactory.getLogger(DdlSqlServices.class);
    //drop table
    //create talbe [name] ( colunms_SQL )
    //add index
    //add primary
    private static final String DROP_TABLE_SQL = " DROP TABLE IF EXISTS %s ";
    private static final String CREATE_TABLE_SQL = " CREATE TABLE %s ( %s ) ";

    private static final String ALTER_TABLE = " ALTER TABLE %s ";
    // ADD PRIMARY KEY ( id )
    private static final String ADD_TABLE_PK ="  ADD PRIMARY KEY ( %s ) ";
    // ADD INDEX `index_hh` (`id`) USING BTREE
    private static final String ADD_TABLE_INDEX =" ADD INDEX  %s  ( %s ) USING  %s ";

    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;
    @Autowired
    private JdbcUtilServices jdbcUtilServices;
    @Autowired
    private Environment env;

    @Transactional
    public boolean execute(String datasource, @NotNull TableInfo tableInfo){
        DbContextHolder.setDBType(datasource);
        log.info("开始建表 :["+tableInfo.getTABLE_NAME()+"],数据源["+datasource+"]");
        String[] sqls = new String[4];
        sqls[0] = createDropTableSql(tableInfo.getTABLE_NAME());
        sqls[1] = createCreateTableSql(tableInfo.getTABLE_NAME(),tableInfo.getColumnInfoList());
        sqls[2] = createAddTablePkSql(tableInfo.getTABLE_NAME(),tableInfo.getPrimaryKeyList());
        sqls[3] = createAddTableIndexSql(tableInfo.getTABLE_NAME(),tableInfo.getIndexInfoList());
        log.info("执行Sql语句: "+ JSON.toJSONString(sqls));
        int[] result = springJdbcTemplate.batchUpdate(sqls);
        log.info("执行结果:" + JSON.toJSONString(result));
        return  true;
    }

    /***
     * 删除表SQL
     * @param tableName
     * @return
     */
    public String createDropTableSql(@NotNull  String tableName ){
        return String.format(DdlSqlServices.DROP_TABLE_SQL,tableName);
    }
    
    public String createCreateTableSql(@NotNull String tableName , @NotEmpty  List<ColumnInfo> columnInfoList){
        String[] colSqls = new String[columnInfoList.size()];
        for (int i = 0; i < columnInfoList.size(); i++) {
            ColumnInfo column = columnInfoList.get(i);

            int colsize  = column.getCOLUMN_SIZE();
            int digits = column.getDECIMAL_DIGITS();

            String defaultValue = column.getCOLUMN_DEF();
            String remarks = column.getREMARKS();
            StringBuffer sbb = new StringBuffer(column.getCOLUMN_NAME()+ " ");
            if("INTEGER".equalsIgnoreCase(column.getTYPE_NAME())){
                //INTEGER -> int
                sbb.append(" int("+colsize+")");
            } else if("SMALLINT".equalsIgnoreCase(column.getTYPE_NAME())){
                //INTEGER -> smallint
                sbb.append(" smallint("+colsize+")");
            }else if("NUMBER".equalsIgnoreCase(column.getTYPE_NAME())){
                if(digits > 0){
                    //number  -> double
                    sbb.append(" double("+colsize+","+digits+")");
                }else{
                    //number -> bigint
                    sbb.append(" bigint("+colsize+")");
                }
            }else if("DATE".equalsIgnoreCase(column.getTYPE_NAME()) || "TIMESTAMP".equalsIgnoreCase(column.getTYPE_NAME())){
                //DATE TIMESTAMP -> datetime
                sbb.append(" datetime ");
                if(StringUtils.isNotEmpty(defaultValue)){
                    if("sysdate".equalsIgnoreCase(defaultValue.trim())){
                        defaultValue = " now() ";
                    }
                }
            }else if("CHAR".equalsIgnoreCase(column.getTYPE_NAME())){
                // VARCHAR2 -> varchar
                sbb.append(" char("+colsize+")");
            }else if("VARCHAR2".equalsIgnoreCase(column.getTYPE_NAME())){
                // VARCHAR2 -> varchar
                sbb.append(" varchar("+colsize+")");
            }else{
                log.error("没有对应的类型处理  type = " +column.getTYPE_NAME());
                log.error("退出");
                return null;
            }

            String canNullSql = column.getNULLABLE()==1 ? " NULL " : " NOT NULL ";
            sbb.append(canNullSql);
            if(StringUtils.isNotEmpty(defaultValue) && !"null".equalsIgnoreCase(defaultValue)){
                sbb.append("DEFAULT " + defaultValue + " " );
            }
            if(StringUtils.isNotEmpty(remarks) && !"null".equalsIgnoreCase(remarks)){
                sbb.append( " COMMENT '"+ remarks +"' " );
            }

            colSqls[i] = sbb.toString();
        }
        String createSql  = String.format(DdlSqlServices.CREATE_TABLE_SQL,tableName, StringUtils.join(colSqls,","));

        return createSql;
    }


    /***
     * 生成创建主键sql
     * @param tableName
     * @param list
     * @return
     */
    public String createAddTablePkSql(String tableName,@NotEmpty List<PrimaryKey> list){
        StringBuffer addPkSql =new StringBuffer(String.format(DdlSqlServices.ALTER_TABLE,tableName));
        String[] pks = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            pks[i] =String.format(DdlSqlServices.ADD_TABLE_PK, list.get(i).getCOLUMN_NAME());
        }
        return addPkSql.append(StringUtils.join(pks,",")).toString();
    }


    /***
     * 生成创建索引的Sql
     * @param tableName
     * @param list
     * @return
     */
    public String createAddTableIndexSql(String tableName,@NotEmpty List<IndexInfo> list){
        StringBuffer addIndexSql =new StringBuffer(String.format(DdlSqlServices.ALTER_TABLE,tableName));
        String[] pks = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            IndexInfo indexInfo = list.get(i);
            pks[i] =String.format(DdlSqlServices.ADD_TABLE_INDEX, indexInfo.getINDEX_NAME(),indexInfo.getCOLUMN_NAME(),"BTREE");
        }
        return addIndexSql.append(StringUtils.join(pks,",")).toString();
    }

    /***
     * 生产oracle序列对应的mysql表
     * @param datasource
     * @param list
     */
    public void executeSeqTable(@NotEmpty  String datasource, List<SequenceInfo> list){
        if(null == list || list.isEmpty()){
            return ;
        }
        log.info("序列表信息:"+ JSON.toJSONString(list));
        DbContextHolder.setDBType(datasource);
        //判断序列表存在不
        String seqTableName = env.getProperty("seq_table_name");
        if(StringUtils.isEmpty(seqTableName)){
            seqTableName = "prv_sequence_gen";
        }
        if(!jdbcUtilServices.existTable(datasource,seqTableName)){
            //新建表  seqTableName
            String creatSeqTableSql = " create table " +seqTableName+ "(" +
                    " id int(11) NOT NULL AUTO_INCREMENT  ," +
                    " sequence_name  varchar(255) DEFAULT NULL ," +
                    " sequence_name_column  varchar(255) DEFAULT NULL ," +
                    " sequence_next_value bigint(20) DEFAULT NULL ," +
                    " PRIMARY KEY (`id`) " +
                    " )";
            springJdbcTemplate.execute(creatSeqTableSql);
        }
        
        String[] insertSql  = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            SequenceInfo sequenceInfo = list.get(i);
            String isql = " INSERT INTO " + seqTableName + " (sequence_name, sequence_next_value) VALUES ('"+sequenceInfo.getOracleSeqName()+"', '"+sequenceInfo.getMaxValue()+"')";
             insertSql[i] = isql;
        }
        int[] result = springJdbcTemplate.batchUpdate(insertSql);
        log.info("序列表插入结果:" + JSON.toJSONString(result));
    }
}
