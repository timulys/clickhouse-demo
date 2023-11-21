package com.example.clickhousedemo.config;

import com.clickhouse.client.*;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * PackageName 	: com.example.clickhousedemo.config
 * FileName 	: ClickHouseConfig
 * Author 		: jhchoi
 * Date 		: 2023-11-20
 * Description 	:
 * ======================================================
 * DATE				    AUTHOR				NOTICE
 * ======================================================
 * 2023-11-20			jhchoi				최초 생성
 */
@Setter
@Configuration
public class ClickHouseServer {
	@Value("${spring.datasource.clickhouse.url}")
	public String url;
	@Value("${spring.datasource.clickhouse.port}")
	public Integer port;
	@Value("${spring.datasource.clickhouse.database}")
	public String database;
	@Value("${spring.datasource.clickhouse.username}")
	public String username;
	@Value("${spring.datasource.clickhouse.password}")
	public String password;

	@Bean
	public ClickHouseRequest clickHouseClient() {
		ClickHouseNode server = ClickHouseNode.builder()
			.host(System.getProperty("chHost", url))
			.port(ClickHouseProtocol.HTTP, Integer.getInteger("chPort", port))
			.database(database).credentials(ClickHouseCredentials.fromUserAndPassword(
				System.getProperty("chUser", username), System.getProperty("chPassword", password)))
			.build();
		return ClickHouseClient.newInstance(server.getProtocol()).read(server);
	}
}


