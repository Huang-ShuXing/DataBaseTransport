package com.maywide.dbt.core.execute;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.druid.proxy.jdbc.ConnectionProxyImpl;
import com.maywide.dbt.config.datasource.dynamic.DbContextHolder;
import com.maywide.dbt.config.datasource.dynamic.DynamicDataSource;
import com.maywide.dbt.core.pojo.oracle.*;
import com.maywide.dbt.core.services.DdlSqlServices;
import com.maywide.dbt.util.SpringJdbcTemplate;
import oracle.jdbc.OracleConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class TableTransport {
    private static final Logger log = LoggerFactory.getLogger(TableTransport.class);

    @Autowired
    private SpringJdbcTemplate springJdbcTemplate;

    @Autowired
    private DdlSqlServices sql ;

    /**
     * 关闭连接
     * @param o
     */
    public static void close(Object o){
        if (o == null){
            return;
        }
        if (o instanceof ResultSet){
            try {
                ((ResultSet)o).close();
            } catch (SQLException e) {
                log.error(e.getMessage(),e);
                e.printStackTrace();
            }
        } else if(o instanceof Statement){
            try {
                ((Statement)o).close();
            } catch (SQLException e) {
                e.printStackTrace();
                log.error(e.getMessage(),e);
            }
        } else if (o instanceof Connection){
            Connection c = (Connection)o;
            try {
                if (!c.isClosed()){
                    c.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                log.error(e.getMessage(),e);
            }
        }
    }

    /***
     * 关闭连接
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(ResultSet rs, Statement stmt, Connection conn){
        close(rs);
        close(stmt);
        close(conn);
    }

    /**
     * 关闭连接
     * @param rs
     * @param conn
     */
    public static void close(ResultSet rs, Connection conn){
        close(rs);
        close(conn);
    }

    /**
     * 获得数据库中所有Schemas(对应于oracle中的Tablespace)
     * @param datasource
     * @throws SQLException
     */
    public  void getSchemasInfo(String datasource) throws SQLException {
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getSchemas();
            while (rs.next()){
                String tableSchem = rs.getString("TABLE_SCHEM");
                //System.out.println(tableSchem);
            }
        } catch (SQLException e){
            e.printStackTrace();
            log.error(e.getMessage(),e);
        } finally{
            TableTransport.close(rs,conn);
        }
    }

    /***
     * 获取数据库连接
     * Oracle连接则增加获取备注设置
     * @param dataSource
     * @return
     * @throws SQLException
     */
    private Connection getConnection(String dataSource) throws SQLException {
        DbContextHolder.setDBType(dataSource);
       Connection conn = springJdbcTemplate.getDataSource().getConnection();
        if (conn instanceof DruidPooledConnection) {//设置Oracle数据库的表注释可读
            Connection cc = ((DruidPooledConnection) conn).getConnection();
            if(cc instanceof ConnectionProxyImpl ){
                ConnectionProxyImpl connectionProxy = (ConnectionProxyImpl) cc;
                Connection c1 = connectionProxy.getConnectionRaw();
                if(c1 instanceof  OracleConnection){
                    ((OracleConnection) c1).setRemarksReporting(true);
                }
            }


        }
        return conn;
    }

    /**
     * 获取数据库相关信息
     * @param datasource
     * @throws SQLException
     */
    public  void getDataBaseInfo(String datasource) throws SQLException {
        Connection conn =  getConnection(datasource);
        ResultSet rs = null;
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            System.out.println("数据库已知的用户: "+ dbmd.getUserName());
            System.out.println("数据库的系统函数的逗号分隔列表: "+ dbmd.getSystemFunctions());
            System.out.println("数据库的时间和日期函数的逗号分隔列表: "+ dbmd.getTimeDateFunctions());
            System.out.println("数据库的字符串函数的逗号分隔列表: "+ dbmd.getStringFunctions());
            System.out.println("数据库供应商用于 'schema' 的首选术语: "+ dbmd.getSchemaTerm());
            System.out.println("数据库URL: " + dbmd.getURL());
            System.out.println("是否允许只读:" + dbmd.isReadOnly());
            System.out.println("数据库的产品名称:" + dbmd.getDatabaseProductName());
            System.out.println("数据库的版本:" + dbmd.getDatabaseProductVersion());
            System.out.println("驱动程序的名称:" + dbmd.getDriverName());
            System.out.println("驱动程序的版本:" + dbmd.getDriverVersion());
            System.out.println("数据库中使用的表类型");
            rs = dbmd.getTableTypes();
            while (rs.next()) {
                System.out.println(rs.getString("TABLE_TYPE"));
            }
        }catch (SQLException e){
            e.printStackTrace();
            log.error(e.getMessage(),e);
        } finally{
            TableTransport.close(rs,conn);
        }
    }

    /***
     * 获取表信息
     * @param datasource
     * @param schema
     * @param tableName 默认*，表示全部表信息
     * @return
     * @throws SQLException
     */
    public List<TableInfo> getTablesList(String datasource, String schema, String tableName)  {
        List<TableInfo> list = null;
        try{
            if("*".equals(tableName)){
                list = this.getTablesList(datasource,schema);
            }else {
                TableInfo tableInfo = this.getTable(datasource,schema,tableName);
                if(null != tableInfo){
                    list = new ArrayList<>();
                    list.add(tableInfo);
                }
            }
        }catch (SQLException se ){
            log.error("获取表信息时出错:"+ se.getMessage(),se);
            list = null;
        }
        return list ;
    }

    /**
     * 获取数据库中所有的表信息
     * @param datasource
     * @param schema
     * @return
     * @throws SQLException
     */
    public List<TableInfo> getTablesList(String datasource,String schema) throws SQLException {
        List<TableInfo> list = new ArrayList<>();
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            String[] types = { "TABLE"};
            rs = dbmd.getTables(null, schema, "%", types);
            while (rs.next()) {
                TableInfo table = new TableInfo();
                String tableName = rs.getString("TABLE_NAME");  //表名
                String tableType = rs.getString("TABLE_TYPE");  //表类型
                String remarks = rs.getString("REMARKS");       //表备注
                //System.out.println(tableName + " - " + tableType + " - " + remarks);
                table.setREMARKS(remarks);
                table.setTABLE_NAME(tableName);
                table.setTABLE_TYPE(tableType);
                table.setSECHEMA(schema);
                list.add(table);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage(),e);
        } finally{
            TableTransport.close(rs,conn);
        }
        return list;
    }


    /***
     * 获取表信息
     * @param datasource
     * @param schema
     * @param tableName 字符 ‘%’ 为模糊匹配
     * @return
     * @throws SQLException
     */
    public TableInfo getTable(String datasource,String schema,String tableName) throws SQLException {
        TableInfo table = null;
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            String[] types = { "TABLE"};
            rs = dbmd.getTables(null, schema, tableName, types);
            while (rs.next()) {
                table = new TableInfo();
                //String tableName = rs.getString("TABLE_NAME");  //表名
                String tableType = rs.getString("TABLE_TYPE");  //表类型
                String remarks = rs.getString("REMARKS");       //表备注
                table.setREMARKS(remarks);
                table.setTABLE_NAME(tableName);
                table.setTABLE_TYPE(tableType);
                table.setSECHEMA(schema);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage(),e);
        } finally{
            TableTransport.close(rs,conn);
        }
        return table;
    }

    //
    //
    //
    //

    /**
     * 数据库表迁移
     * 1.连接oracle
     * 2.查询指定表的结构
     * 3.根据每张表的结构构造MYSQL 的建表语句
     * 4.连接mysql，建表
     *
     * @param schema
     * @param datasource
     * @param table
     * @return
     */
    public  boolean execute(String schema,String datasource,String table){
        boolean flag  = false;
        try {
            if("*".equals(table)){
                log.info("开始加载表信息");
                List<TableInfo> list = this.getTablesList(datasource,schema);

                if(null != list && !list.isEmpty()){
                    log.info("UAPDB 下的全部数据库数 :" +list.size() );
                    for (TableInfo tableInfo : list) {

                        log.info("开始加载某个主键信息");
                        tableInfo.setPrimaryKeyList(this.getPrimaryKeysInfo(datasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

                        log.info("开始加载某个表索引信息");
                        tableInfo.setIndexInfoList(this.getIndexInfo(datasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

                        log.info("开始加载某个表列信息");
                        tableInfo.setColumnInfoList(this.getColumnsInfo(datasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

                        log.info(tableInfo.toString());
                    }
                    //开始执行 mysql 建表
                    log.info("开始建MYSQL 数据库表");
                    for (String mysqlDb : DynamicDataSource.otherDataSource) {
                        for (TableInfo tableInfo : list) {
                            sql.execute(mysqlDb,tableInfo);
                        }
                    }
                    flag =true;
                }else {
                    log.warn("数据{"+datasource+"},schema{"+schema+"}中没有任何表信息");
                    flag =false;
                }
            }else {
                TableInfo tableInfo = this.initTable(datasource,schema,table);
                if(null != tableInfo){
                    for (String mysqlDb : DynamicDataSource.otherDataSource) {
                        this.copyTableToMySql(mysqlDb,tableInfo);
                    }
                    flag = true;
                }else {
                    log.error("没有找到指定的table，退出了");
                    flag = false;
                }
            }
        } catch (SQLException e) {
            log.error("oracle 表 迁移 到mysql 表过程报错:"+e.getMessage(),e);
            flag = false;
        }
        return flag;
    }

    /***
     * 将oracle 序列转存到mysql 一张序列表
     * @param datasource
     * @param schema
     */
    public boolean transprotOracleSeqToMysqlTable(String datasource,String schema) {
        log.info("处理表的序列问题");
        //oracle序列可以转id 自增长;这里的逻辑不转自增长，转成 表存序列
        List<SequenceInfo> seqList =this.getSchemaSeqInfo(datasource,schema);

        //开始执行 mysql 建表
        log.info("开始将oracle 序列 转换为 mysql 数据库表");
        for (String mysqlDb : DynamicDataSource.otherDataSource) {
            //建立序列表
            sql.executeSeqTable(mysqlDb,seqList);
        }
        return true;
    }


    /***
     * 获取数据库的序列信息
     * @param datasource
     * @param schema
     * @return
     */
    private List<SequenceInfo> getSchemaSeqInfo(String datasource,String schema){

        String seqSql = " select sequence_name,last_number from user_sequences";
        DbContextHolder.setDBType(datasource);
        List<Map<String,Object>> list = springJdbcTemplate.queryForList(seqSql);
        List<SequenceInfo> seqList = new ArrayList<>(list.size());
        if(null != list && !list.isEmpty()){
            for (Map<String, Object> oneResult : list) {
                String seqName = (String) oneResult.get("sequence_name");
                BigDecimal db_max = (BigDecimal) oneResult.get("last_number");
                long max =db_max.longValue();
                SequenceInfo  seq = new SequenceInfo();
                seq.setOracleSeqName(seqName);
                seq.setMaxValue(max);
                seqList.add(seq);
            }
        }else{
            log.error("获取的序列为空,数据库={"+datasource+"}, schema = {"+schema+"}");
        }
        return seqList;
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
            TableTransport.close(rs,con);
        }
        log.info("判断表存在 - 表 "+tableName +"在不在 数据库["+inDataSource+"]中 = 【"+isExist+"】");
        return isExist;
    }
//
//
//    public  String getTableContent(String tableName) {
//        String content = "";
//        if ((tableName != null) && (!tableName.equals(""))) {
//            Connection conn = null;
//            Statement stmt = null;
//            ResultSet rs = null;
//            DatabaseMetaData dbMetaData = null;
//            ResultSet resultSet = null;
//            try {
//                conn = springJdbcTemplate.getDataSource().getConnection();
//                if (conn instanceof DruidPooledConnection) {//设置Oracle数据库的表注释可读
//                    log.info("oracleConnection");
//                    Connection cc =((DruidPooledConnection) conn).getConnection();
//                    ConnectionProxyImpl connectionProxy = (ConnectionProxyImpl) cc;
//                    Connection c1 = connectionProxy.getConnectionRaw();
//                    System.out.println(c1.getClass());
//                    ((OracleConnection) c1).setRemarksReporting(true);
//                }
//                dbMetaData = conn.getMetaData();
//                stmt = conn.createStatement();
//                rs = stmt.executeQuery("select * from " + tableName);
//                ResultSetMetaData rsmd = rs.getMetaData();
//                int columnCount = rsmd.getColumnCount();
//
//                //String indexStr = DbUtils.getIndexStr(springJdbcTemplate,tableName);
//                //String pkStr = DbUtils.getPkStr(tableName,springJdbcTemplate);
//                //boolean isEmbeddedId = pkStr.split(";").length > 1;
//
//
//                for (int i = 1; i < columnCount + 1; i++) {
//                    String colName = rsmd.getColumnName(i);
//                    String colName2 = rsmd.getColumnName(i);
//                    String colType = rsmd.getColumnTypeName(i);
//                    String type = rsmd.getColumnTypeName(i);
//                    String ss = rsmd.getCatalogName(i);
//                    String label = rsmd.getColumnLabel(i);
//                    boolean isAutoInc = rsmd.isAutoIncrement(i);
//                    /*resultSet = dbMetaData.getColumns(
//                            rsmd.getCatalogName(i), rsmd.getSchemaName(i),
//                            rsmd.getTableName(i), rsmd.getColumnName(i));*/
//
//                    resultSet = dbMetaData.getColumns(
//                            null,"UAPDB",
//                            tableName, rsmd.getColumnName(i));
//
//                    log.info(rsmd.getCatalogName(i)+","+rsmd.getSchemaName(i)+","+rsmd.getTableName(i));
//                    String remarks = rsmd.getColumnName(i);//如果列没有备注，默认为列名称
//                    while (null != resultSet &&resultSet.next()) {
//                        if(StringUtils.isNotBlank(resultSet.getString("REMARKS"))){
//                            remarks = resultSet.getString("REMARKS");
//                        }
//                    }
//                    int isNullable = rsmd.isNullable(i);
//                    int colLength = rsmd.getColumnDisplaySize(i);
//                    int precision = rsmd.getPrecision(i);
//                    int scale = rsmd.getScale(i);
//
//
//                    if (indexStr.indexOf(colName2.toUpperCase()) != -1) {
//                        colName = "index_" + colName;
//                    }
//
//                    if (pkStr.indexOf(colName2) != -1) {
//                        colName = "pk_" + colName;
//                        if (isEmbeddedId) {
//                            colName = "pk_" + colName;
//                        }
//                    }
//
//                    if ((colType.equalsIgnoreCase("INTEGER"))
//                            || (colType.equalsIgnoreCase("int"))) {
//                        colType = "Long";
//                    } else if ((colType.equalsIgnoreCase("NUMBER"))
//                            && (scale == 0)) {
//                        colType = "Long";
//                    } else if ((colType.equalsIgnoreCase("NUMBER"))
//                            && (scale != 0)) {
//                        colType = "Double";
//                    } else if ((colType.equalsIgnoreCase("VARCHAR"))
//                            || ((colType.equalsIgnoreCase("VARCHAR2") | colType
//                            .equalsIgnoreCase("CHAR")))
//                            || (colType.equalsIgnoreCase("LONG VARCHAR"))) {
//                        colType = "String";
//                    } else if (colType.equalsIgnoreCase("BIGINT")) {
//                        colType = "Long";
//                    } else if (colType.equalsIgnoreCase("FLOAT")) {
//                        colType = "Double";
//                    } else if (colType.equalsIgnoreCase("CLOB")) {
//                        colType = "String";
//                    } else if ((colType.equalsIgnoreCase("date"))
//                            || (colType.equalsIgnoreCase("timestamp"))) {
//                        colType = "java.util.Date";
//                        type = "Date";
//                    } else {
//                        throw new Exception(colType + "类型还没考虑，请联系管理员");
//                    }
//
//                    if (i == columnCount) {
//                        content = content + colName.toLowerCase() + ":"
//                                + colType + ":" + type + ":" + isNullable + ":"
//                                + colLength + ":" + precision + ":" + scale + ":" + remarks + ":"+label +":"+isAutoInc;
//                    } else{
//                        content = content + colName.toLowerCase() + ":"
//                                + colType + ":" + type + ":" + isNullable + ":"
//                                + colLength + ":" + precision + ":" + scale + ":" + remarks + ":"+label+":"+isAutoInc
//                                + "\r\n";
//                    }
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                DbUtils.closeAll(conn, stmt, rs);
//            }
//        }
//        return content;
//    }

    /**
     * @Description: 获取某表信息
     * @author: chenzw
     * @CreateTime: 2014-1-27 下午3:26:30
     * @throws
     */
    /*public void getTablesInfo(String datasource , String tablename) throws SQLException {
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            *//**
             * 获取给定类别中使用的表的描述。
             * 方法原型:ResultSet getTables(String catalog,String schemaPattern,String tableNamePattern,String[] types);
             * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
             * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
             * tableNamePattern - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
             * types - 表类型数组; "TABLE"、"VIEW"、"SYSTEM TABLE"、"GLOBAL TEMPORARY"、"LOCAL TEMPORARY"、"ALIAS" 和 "SYNONYM";null表示包含所有的表类型;可包含单字符通配符("_"),或多字符通配符("%");
             *//*
            rs = dbmd.getTables(null, null, tablename, new String[]{"TABLE"});

            while(rs.next()){
                String tableCat = rs.getString("TABLE_CAT");  //表类别(可为null)
                String tableSchemaName = rs.getString("TABLE_SCHEM");//表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
                String tableName = rs.getString("TABLE_NAME");  //表名
                String tableType = rs.getString("TABLE_TYPE");  //表类型,典型的类型是 "TABLE"、"VIEW"、"SYSTEM TABLE"、"GLOBAL TEMPORARY"、"LOCAL TEMPORARY"、"ALIAS" 和 "SYNONYM"。
                String remarks = rs.getString("REMARKS");       //表备注

                System.out.println(tableCat + " - " + tableSchemaName + " - " +tableName + " - " + tableType + " - "
                        + remarks);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }finally{
            TableTransport.close(rs,conn);
        }
    }*/

    /**
     * 获取表主键信息
     * @param datasource
     * @param sechema
     * @param tablename
     * @return
     * @throws SQLException
     */
    public  List<PrimaryKey> getPrimaryKeysInfo(String datasource, String sechema, String tablename) throws SQLException {
        List<PrimaryKey> list = new ArrayList<>();
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            /**
             * 获取对给定表的主键列的描述
             * 方法原型:ResultSet getPrimaryKeys(String catalog,String schema,String table);
             * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
             * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
             * table - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
             */
            rs = dbmd.getPrimaryKeys(null, sechema, tablename);

            while (rs.next()){
                PrimaryKey pk = new PrimaryKey();
                String tableCat = rs.getString("TABLE_CAT");  //表类别(可为null)
                String tableSchemaName = rs.getString("TABLE_SCHEM");//表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
                String tableName = rs.getString("TABLE_NAME");  //表名
                String columnName = rs.getString("COLUMN_NAME");//列名
                short keySeq = rs.getShort("KEY_SEQ");//序列号(主键内值1表示第一列的主键，值2代表主键内的第二列)
                String pkName = rs.getString("PK_NAME"); //主键名称

//                System.out.println(tableCat + " - " + tableSchemaName + " - " + tableName + " - " + columnName + " - "
//                        + keySeq + " - " + pkName);
                pk.setCOLUMN_NAME(columnName);
                pk.setTABLE_NAME(tableName);
                pk.setKEY_SEQ(keySeq);
                pk.setPK_NAME(pkName);
                list.add(pk);
            }
        }catch (SQLException e){
            e.printStackTrace();
            log.error(e.getMessage(),e);
        }finally{
            TableTransport.close(rs,conn);
        }
        return list;
    }

    /**
     * 获取表序列
     * @param datasource
     * @param sechema
     * @param tablename
     * @return
     * @throws SQLException
     */
    public  List<IndexInfo> getIndexInfo(String datasource, String sechema, String tablename) throws SQLException {
        List<IndexInfo> list = new ArrayList<>();
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;
        try{
            DatabaseMetaData dbmd = conn.getMetaData();
            /**
             * 获取给定表的索引和统计信息的描述
             * 方法原型:ResultSet getIndexInfo(String catalog,String schema,String table,boolean unique,boolean approximate)
             * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
             * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
             * table - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
             * unique - 该参数为 true时,仅返回唯一值的索引; 该参数为 false时,返回所有索引;
             * approximate - 该参数为true时,允许结果是接近的数据值或这些数据值以外的值;该参数为 false时,要求结果是精确结果;
             */
            rs = dbmd.getIndexInfo(null, sechema, tablename, false, true);
            while (rs.next()){
                String tableCat = rs.getString("TABLE_CAT");  //表类别(可为null)
                String tableSchemaName = rs.getString("TABLE_SCHEM");//表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
                String tableName = rs.getString("TABLE_NAME");  //表名
                boolean nonUnique = rs.getBoolean("NON_UNIQUE");// 索引值是否可以不唯一,TYPE为 tableIndexStatistic时索引值为 false;
                String indexQualifier = rs.getString("INDEX_QUALIFIER");//索引类别（可能为空）,TYPE为 tableIndexStatistic 时索引类别为 null;
                String indexName = rs.getString("INDEX_NAME");//索引的名称 ;TYPE为 tableIndexStatistic 时索引名称为 null;
                /**
                 * 索引类型：
                 *  tableIndexStatistic - 此标识与表的索引描述一起返回的表统计信息
                 *  tableIndexClustered - 此为集群索引
                 *  tableIndexHashed - 此为散列索引
                 *  tableIndexOther - 此为某种其他样式的索引
                 */
                short type = rs.getShort("TYPE");//索引类型;
                short ordinalPosition = rs.getShort("ORDINAL_POSITION");//在索引列顺序号;TYPE为 tableIndexStatistic 时该序列号为零;
                String columnName = rs.getString("COLUMN_NAME");//列名;TYPE为 tableIndexStatistic时列名称为 null;
                String ascOrDesc = rs.getString("ASC_OR_DESC");//列排序顺序:升序还是降序[A:升序; B:降序];如果排序序列不受支持,可能为 null;TYPE为 tableIndexStatistic时排序序列为 null;
                int cardinality = rs.getInt("CARDINALITY");   //基数;TYPE为 tableIndexStatistic 时,它是表中的行数;否则,它是索引中唯一值的数量。
                int pages = rs.getInt("PAGES"); //TYPE为 tableIndexStatisic时,它是用于表的页数,否则它是用于当前索引的页数。
                String filterCondition = rs.getString("FILTER_CONDITION"); //过滤器条件,如果有的话(可能为 null)。

                /*System.out.println(tableCat + " - " + tableSchemaName + " - " + tableName + " - " + nonUnique + " - "
                        + indexQualifier + " - " + indexName + " - " + type + " - " + ordinalPosition + " - " + columnName
                        + " - " + ascOrDesc + " - " + cardinality + " - " + pages + " - " + filterCondition);
*/
                //过滤掉 type 为 0 column 为null 的索引
                if(0 == type && (columnName == null || columnName.equals("null"))){
                    continue;
                }
                IndexInfo indexInfo = new IndexInfo();
                indexInfo.setCOLUMN_NAME(columnName);
                indexInfo.setINDEX_NAME(indexName);
                indexInfo.setNON_UNIQUE(nonUnique);
                list.add(indexInfo);
            }
        } catch (SQLException e){
            e.printStackTrace();
            log.error(e.getMessage(),e);
        } finally{
            TableTransport.close(rs,conn);
        }
        return list;
    }

    /**
     * 获取表的列信息
     * @param datasource
     * @param schema
     * @param tablename
     * @return
     * @throws SQLException
     */
    public  List<ColumnInfo> getColumnsInfo(String datasource, String schema, String tablename) throws SQLException {
        List<ColumnInfo> list = new ArrayList<>();
        Connection conn =  this.getConnection(datasource);
        ResultSet rs = null;

        try{
            /**
             * 设置连接属性,使得可获取到列的REMARK(备注)
             */
            DatabaseMetaData dbmd = conn.getMetaData();
            /**
             * 获取可在指定类别中使用的表列的描述。
             * 方法原型:ResultSet getColumns(String catalog,String schemaPattern,String tableNamePattern,String columnNamePattern)
             * catalog - 表所在的类别名称;""表示获取没有类别的列,null表示获取所有类别的列。
             * schema - 表所在的模式名称(oracle中对应于Tablespace);""表示获取没有模式的列,null标识获取所有模式的列; 可包含单字符通配符("_"),或多字符通配符("%");
             * tableNamePattern - 表名称;可包含单字符通配符("_"),或多字符通配符("%");
             * columnNamePattern - 列名称; ""表示获取列名为""的列(当然获取不到);null表示获取所有的列;可包含单字符通配符("_"),或多字符通配符("%");
             */
            rs =dbmd.getColumns(null, schema, tablename, null);

            while(rs.next()){
                String tableCat = rs.getString("TABLE_CAT");  //表类别（可能为空）
                String tableSchemaName = rs.getString("TABLE_SCHEM");  //表模式（可能为空）,在oracle中获取的是命名空间,其它数据库未知
                String tableName_ = rs.getString("TABLE_NAME");  //表名
                String columnName = rs.getString("COLUMN_NAME");  //列名
                int dataType = rs.getInt("DATA_TYPE");     //对应的java.sql.Types的SQL类型(列类型ID)
                String dataTypeName = rs.getString("TYPE_NAME");  //java.sql.Types类型名称(列类型名称)
                int columnSize = rs.getInt("COLUMN_SIZE");  //列大小
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");  //小数位数
                int numPrecRadix = rs.getInt("NUM_PREC_RADIX");  //基数（通常是10或2） --未知
                /**
                 *  0 (columnNoNulls) - 该列不允许为空
                 *  1 (columnNullable) - 该列允许为空
                 *  2 (columnNullableUnknown) - 不确定该列是否为空
                 */
                int nullAble = rs.getInt("NULLABLE");  //是否允许为null
                String remarks = rs.getString("REMARKS");  //列描述
                String columnDef = rs.getString("COLUMN_DEF");  //默认值
                int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");    // 对于 char 类型，该长度是列中的最大字节数
                int ordinalPosition = rs.getInt("ORDINAL_POSITION");   //表中列的索引（从1开始）
                /**
                 * ISO规则用来确定某一列的是否可为空(等同于NULLABLE的值:[ 0:'YES'; 1:'NO'; 2:''; ])
                 * YES -- 该列可以有空值;
                 * NO -- 该列不能为空;
                 * 空字符串--- 不知道该列是否可为空
                 */
                String isNullAble = rs.getString("IS_NULLABLE");

                /**
                 * 指示此列是否是自动递增
                 * YES -- 该列是自动递增的
                 * NO -- 该列不是自动递增
                 * 空字串--- 不能确定该列是否自动递增
                 */
                //String isAutoincrement = rs.getString("IS_AUTOINCREMENT");   //该参数测试报错
//                System.out.println(tableCat + " - " + tableSchemaName + " - " + tableName_ + " - " + columnName +
//                        " - " + dataType + " - " + dataTypeName + " - " + columnSize + " - " + decimalDigits + " - "
//                        + numPrecRadix + " - " + nullAble + " - " + remarks + " - " + columnDef + " - " + charOctetLength
//                        + " - " + ordinalPosition + " - " + isNullAble );

                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setTABLE_CAT(tableCat);
                columnInfo.setTABLE_NAME(tableName_);
                columnInfo.setCOLUMN_NAME(columnName);
                columnInfo.setDATA_TYPE(dataType);
                columnInfo.setTYPE_NAME(dataTypeName);
                columnInfo.setCOLUMN_SIZE(columnSize);
                columnInfo.setDECIMAL_DIGITS(decimalDigits);
                columnInfo.setNULLABLE(nullAble);
                columnInfo.setREMARKS(remarks);
                columnInfo.setCOLUMN_DEF(columnDef);
                columnInfo.setCHAR_OCTET_LENGTH(charOctetLength);
                columnInfo.setORDINAL_POSITION(ordinalPosition);
                columnInfo.setIS_NULLABLE(isNullAble);
                list.add(columnInfo);
            }
        }catch(SQLException ex){
            ex.printStackTrace();
        }finally{
            TableTransport.close(rs,conn);
        }
        return list;
    }


    /***
     * 初始化表信息
     * @param oriDatasource
     * @param tableName
     */
    public TableInfo initTable(String oriDatasource,String schema,String tableName) throws SQLException {

        TableInfo tableInfo = this.getTable(oriDatasource,schema,tableName);
        if(tableInfo == null){
            log.error("找不到表:talbename = " + tableName);
            return tableInfo;
        }
        log.info("开始加载表["+tableName+"]主键信息");
        tableInfo.setPrimaryKeyList(getPrimaryKeysInfo(oriDatasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

        log.info("开始加载表["+tableName+"]索引信息");
        tableInfo.setIndexInfoList(getIndexInfo(oriDatasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

        log.info("开始加载表["+tableName+"]列信息");
        tableInfo.setColumnInfoList(getColumnsInfo(oriDatasource,tableInfo.getSECHEMA(),tableInfo.getTABLE_NAME()));

        return tableInfo;
    }

    /***
     * 根据表信息 复制到mysql 数据库
     * @param targetDatasource
     * @param tableInfo
     */
    public void copyTableToMySql(String targetDatasource , TableInfo tableInfo){
        sql.execute(targetDatasource,tableInfo);
    }


    /***
     * 查询数据库名
     *
     * @param datasource
     * @return 返回 Oracle,Mysql
     * @throws SQLException
     */
    public String getDbName(String datasource) throws SQLException {
        String dbProductName =  null;
        Connection conn =  this.getConnection(datasource);
        try{
            dbProductName = conn.getMetaData().getDatabaseProductName();
        }catch (SQLException e){
            e.printStackTrace();
            log.error(e.getMessage(),e);
        }finally{
            TableTransport.close(conn);
        }
        return dbProductName;
    }

}
