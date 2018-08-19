package com.maywide.dbt.core.services;

import com.alibaba.druid.support.json.JSONUtils;
import com.alibaba.fastjson.JSON;
import com.maywide.dbt.config.datasource.dynamic.DbContextHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Service
public class TestServices {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public  String testOneService(){

        String allDataBase = "select * from bi_data_source where c_name in ('61node1','60node2','60node1')";
        List<Map<String, Object>> dataSource = jdbcTemplate.queryForList(allDataBase);
        for (Map<String, Object> stringObjectMap : dataSource) {
            if(stringObjectMap.containsKey("c_name")){
                String c_name = (String) stringObjectMap.get("c_name");
                System.out.println("数据库=" + c_name);
                DbContextHolder.setDBType(c_name);
                List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from biz_sku");
                System.out.println(JSON.toJSONString(list));
            }

        }

        /*DbContextHolder.setDBType("60node1");
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from prd_sales");

        System.out.println(JSONUtils.toJSONString(list));*/
        return "";//JSONUtils.toJSONString(list);
    }


    public void testInsert(){

        String maxId = " select max(id) from test_bi_sku";
        String inSql = " INSERT INTO `test_bi_sku` (`saleisid`, `skuid`, `fees`, `servid`, `stime`, `etime`) VALUES (?, ?, ?, ?, ?,?) ";
        String allDataBase = "select * from bi_data_source";
        List<Map<String, Object>> dataSource = jdbcTemplate.queryForList(allDataBase);
        for (Map<String, Object> stringObjectMap : dataSource) {
            if(stringObjectMap.containsKey("c_name")){
                String c_name = (String) stringObjectMap.get("c_name");
                System.out.println("数据库=" + c_name);
                DbContextHolder.setDBType(c_name);
                Long maxid = jdbcTemplate.queryForObject(maxId,Long.class);
                System.out.println("maxid = " + maxid );
                jdbcTemplate.update(inSql,Math.random(),Math.random(),Math.random(),Math.random(),new Date(),new Date());
                List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from biz_sku");
                System.out.println(JSONUtils.toJSONString(list));
            }

        }

    }


}
