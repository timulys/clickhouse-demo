package com.example.clickhousedemo;

import com.example.clickhousedemo.service.ClickHouseService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class ClickhouseDemoApplication {
	public static void main(String[] args) {
		ConfigurableApplicationContext context =
				SpringApplication.run(ClickhouseDemoApplication.class, args);
		ClickHouseService service = context.getBean(ClickHouseService.class);

		String table = "demo_table";
		// 기존 테이블이 있다면 테이블을 삭제 후 다시 생성합니다.
		service.dropAndCreateTable(table);
		// 테이블에 데이터 10000 Row Insert
		service.insert(table);
		System.out.println("Row Count = " + service.query(table));
	}
}
