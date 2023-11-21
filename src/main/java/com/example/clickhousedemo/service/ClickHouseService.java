package com.example.clickhousedemo.service;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.BinaryStreamUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
	// Request Client Audotwired
	private final ClickHouseRequest client;

	// Drop table and create table
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

	// Data insert
	public long insert(String table) {
		try {
			ClickHouseRequest.Mutation request = client.write().table(table).format(ClickHouseFormat.RowBinary);
			ClickHouseConfig config = request.getConfig();
			CompletableFuture<ClickHouseResponse> future;
			try (ClickHousePipedOutputStream stream =
						 ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(config, (Runnable) null)) {
				future = request.data(stream.getInputStream()).execute();

				// 10000 Rows 데이터 insert
				for (int i = 0; i < 10000000; i++) {
					BinaryStreamUtils.writeString(stream, String.valueOf(i % 16));
					BinaryStreamUtils.writeNonNull(stream);
					BinaryStreamUtils.writeString(stream, UUID.randomUUID().toString());
				}
			}

			// CompletableFuture 를 활용한 비동기 콜백
			try (ClickHouseResponse response = future.get()) {
				ClickHouseResponseSummary summary = response.getSummary();
				return summary.getWrittenRows();
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException | IOException e) {
			throw new RuntimeException(e);
		}
	}

	// Data select
	public int query(String table) {
		try (ClickHouseResponse response = (ClickHouseResponse) client.format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
				.query("select * from " + table).execute().get()) {
			int count = 0;
			for (ClickHouseRecord record : response.records()) {
				count++;
			}
			return count;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		} catch (ExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
