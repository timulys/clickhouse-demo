package com.example.clickhousedemo.service;

import com.clickhouse.client.ClickHouseRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

/**
 * PackageName 	: com.example.clickhousedemo.service
 * FileName 	: ClickHouseService
 * Author 		: jhchoi
 * Date 		: 2023-11-21
 * Description 	:
 * ======================================================
 * DATE				    AUTHOR				NOTICE
 * ======================================================
 * 2023-11-21			jhchoi				최초 생성
 */
@Service
@RequiredArgsConstructor
public class ClickHouseService {
	private final ClickHouseRequest client;

	public void dropAndCreateTable(String table) {
		try {
			ClickHouseRequest<?> request = client;
			request.query("drop table if exists " + table).execute().get();
			request.query("create table " + table + "(a String, b Nullable(String)) engine=MergeTree() order by a")
				.execute().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public long insert(String table) {

	}
}
