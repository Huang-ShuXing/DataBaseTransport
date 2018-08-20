package com.maywide.dbt;

import com.alibaba.fastjson.JSON;
import com.maywide.dbt.core.execute.DataTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
//注册动态多数据源
//@Import({DynamicDataSource.class})
public class Application {

	private static final Logger log = LoggerFactory.getLogger(Application.class);
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true){
					log.info("线程 {"+Thread.currentThread().getName()+"},"+ JSON.toJSONString(DataTransport.dataCopyPoolExecutor));
					try {
						Thread.sleep(3*60*1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						log.error(e.getMessage(),e);
					}
				}
			}
		}){
		}.start();;

	}
}
