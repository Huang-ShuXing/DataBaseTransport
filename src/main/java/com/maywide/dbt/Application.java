package com.maywide.dbt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//注册动态多数据源
//@Import({DynamicDataSource.class})
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);

	}
}
