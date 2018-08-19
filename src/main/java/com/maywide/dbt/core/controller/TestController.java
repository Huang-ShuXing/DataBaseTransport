package com.maywide.dbt.core.controller;

import com.maywide.dbt.core.services.TestServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private TestServices testServices;

    @RequestMapping("/one")
    public String testOne(){
        return testServices.testOneService();
    }

    @RequestMapping("/insert")
    public String testIn (){
        testServices.testInsert();
        return "";
    }
}
