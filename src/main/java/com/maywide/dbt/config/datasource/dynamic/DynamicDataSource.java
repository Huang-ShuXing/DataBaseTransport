package com.maywide.dbt.config.datasource.dynamic;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang.StringUtils;
import org.hibernate.annotations.AttributeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import org.springframework.jdbc.datasource.lookup.JndiDataSourceLookup;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class DynamicDataSource extends AbstractRoutingDataSource {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private Map<Object, Object> _targetDataSources;

    public static final List<String> otherDataSource = new ArrayList<>();


    @Autowired
    private Environment env ;
    /**
     * @see org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource#determineCurrentLookupKey()
     * @describe 数据源为空或者为0时，自动切换至默认数据源，即在配置文件中定义的dataSource数据源
     */
    @Override
    protected Object determineCurrentLookupKey() {
        String dataSourceName = DbContextHolder.getDBType();
        if (dataSourceName == null) {
            dataSourceName = Constants.DEFAULT_DATA_SOURCE_NAME;
        } else {
            this.selectDataSource(dataSourceName);
        }
        log.debug("--------> use datasource " + dataSourceName);
        return dataSourceName;
    }

    @Override
    public void afterPropertiesSet() {
        if(null ==_targetDataSources){
            this._targetDataSources = new HashMap<>();
            initDefaultDatasource(env);
            initOtherDatasource(env);
            //this.initAllConfigDatasource(dataSource);
            //this._targetDataSources.put(Constants.DEFAULT_DATA_SOURCE_NAME,dataSource);
        }
        super.setTargetDataSources(this._targetDataSources);
        super.afterPropertiesSet();
    }

    /***
     * 初始化默认数据源
     * @param env
     * @return
     */
    private DataSource initDefaultDatasource(Environment env){
        String driverClassName = env.getProperty("spring.datasource.driverClassName");
        String url = env.getProperty("spring.datasource.url");
        String userName = env.getProperty("spring.datasource.username");
        String password = env.getProperty("spring.datasource.password");
        String dataSourceName = env.getProperty("spring.datasource.name");
        DataSource dataSource = this.createDataSource(driverClassName, url, userName, password,dataSourceName);
        this._targetDataSources.put(Constants.DEFAULT_DATA_SOURCE_NAME,dataSource);
        return dataSource;
    }

    /***
     * 初始化 额外数据源
     * @param env
     */
    private void initOtherDatasource(Environment env){
        log.info("初始化 数据库中配置的全部数据库连接");
        String prefix = "target.mysql.datasource";
        String otherDbNames = env.getProperty(prefix+".names");
        if(StringUtils.isEmpty(otherDbNames)){
            log.info("未配置其他数据源");
            return ;
        }

        for (String dbname : otherDbNames.split(",")) {
            String driverClassName = env.getProperty(prefix+"."+dbname+".driverClassName");
            String url = env.getProperty(prefix+"."+dbname+".url");
            String userName = env.getProperty(prefix+"."+dbname+".username");
            String password = env.getProperty(prefix+"."+dbname+".password");
            DataSource dataSource = this.createDataSource(driverClassName, url, userName, password,dbname);
            this._targetDataSources.put(dbname,dataSource);
            DynamicDataSource.otherDataSource.add(dbname);
        }
    }


    /**
     * 到数据库中查找名称为dataSourceName的数据源
     * @param dataSourceName
     */
    private void selectDataSource(String dataSourceName) {
        Object sid = DbContextHolder.getDBType();
        if (StringUtils.isEmpty(dataSourceName)
                || dataSourceName.trim().equals(Constants.DEFAULT_DATA_SOURCE_NAME)) {
            DbContextHolder.setDBType(Constants.DEFAULT_DATA_SOURCE_NAME);
            return;
        }
        Object obj = this._targetDataSources.get(dataSourceName);
        if (obj != null && sid.equals(dataSourceName)) {
            return;
        } else {
            DataSource dataSource = this.getDataSource(dataSourceName);
            if (null != dataSource) {
                this.setDataSource(dataSourceName, dataSource);
            }
        }
    }

    @Override
    public void setTargetDataSources(Map<Object, Object> targetDataSources) {
        this._targetDataSources = targetDataSources;
        super.setTargetDataSources(this._targetDataSources);
        super.afterPropertiesSet();
    }

    private void addTargetDataSource(String key, DataSource dataSource) {
        this._targetDataSources.put(key, dataSource);
        this.setTargetDataSources(this._targetDataSources);
    }

//    @Bean
//    @ConfigurationProperties(prefix="spring.datasource")
//    public DruidDataSource getMyDataSource(){
//        return new DruidDataSource();
//    }

    public DataSource createDataSource(String driverClassName, String url,
                                        String username, String password,String dataSourceName) {
        DruidDataSource dataSource = new DruidDataSource();//(DruidDataSource) getMyDataSource();
        dataSource.configFromPropety(DuridConfig.getProperties());
        dataSource.setName(dataSourceName);
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        try {
            System.out.println(env.getProperty("spring.datasource.","initial-size"));
            System.out.println(env.getProperty("spring.datasource.initial-size"));
            dataSource.setFilters("stat,wall");
            dataSource.setMaxActive(80);
            dataSource.setMinIdle(30);
            dataSource.setInitialSize(5);
            dataSource.setTestOnBorrow(true);
            dataSource.setValidationQuery("select 1 from dual");
            dataSource.setMaxWait(10000);
            dataSource.setTestWhileIdle(true);
            dataSource.setTimeBetweenEvictionRunsMillis(18800);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e.getMessage(),e);
        }
        //dataSource.setInitialSize(5);
        return dataSource;
    }

    /**
     * 到数据库中查询名称为dataSourceName的数据源
     *
     * @author Geloin
     * @date Jan 20, 2014 12:18:12 PM
     * @param dataSourceName
     * @return
     */
    private DataSource getDataSource(String dataSourceName) {
        this.selectDataSource(Constants.DEFAULT_DATA_SOURCE_NAME);
        this.determineCurrentLookupKey();
        Connection conn = null;
        try {
            conn = this.getConnection();
            StringBuilder builder = new StringBuilder();
            builder.append("SELECT C_NAME,C_TYPE,C_URL,C_USER_NAME,");
            builder.append("C_PASSWORD,C_JNDI_NAME,C_DRIVER_CLASS_NAME ");
            builder.append("FROM BI_DATA_SOURCE WHERE c_name = ?");

            PreparedStatement ps = conn.prepareStatement(builder.toString());
            ps.setString(1, dataSourceName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {

                Integer type = rs.getInt("C_TYPE");
                if (StringUtils.isNotEmpty(String.valueOf(type))) {
                    // DB
                    String url = rs.getString("C_URL");
                    String userName = rs.getString("C_USER_NAME");
                    String password = rs.getString("C_PASSWORD");
                    String driverClassName = rs
                            .getString("C_DRIVER_CLASS_NAME");
                    String name = rs.getString("C_NAME");
                    DataSource dataSource = this.createDataSource(
                            driverClassName, url, userName, password,name);
                    return dataSource;
                } else {
                    // JNDI
                    String jndiName = rs.getString("C_JNDI_NAME");

                    JndiDataSourceLookup jndiLookUp = new JndiDataSourceLookup();
                    DataSource dataSource = jndiLookUp.getDataSource(jndiName);
                    return dataSource;
                }

            }
            rs.close();
            ps.close();
        } catch (SQLException e) {
            log.error(String.valueOf(e));
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error(String.valueOf(e));
            }
        }
        return null;
    }

    /**
     * 将已存在的数据源存储到内存中
     * @param dataSourceName
     * @param dataSource
     */
    private void setDataSource(String dataSourceName, DataSource dataSource) {
        this.addTargetDataSource(dataSourceName, dataSource);
        DbContextHolder.setDBType(dataSourceName);
    }


    public DataSource getAcuallyDataSource() {
        Object lookupKey = determineCurrentLookupKey();
        if(null == lookupKey) {
            return this;
        }
        DataSource determineTargetDataSource = this.determineTargetDataSource();
        return determineTargetDataSource==null ? this : determineTargetDataSource;
    }



}