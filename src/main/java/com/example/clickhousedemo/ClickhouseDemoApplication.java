package com.example.clickhousedemo;

import com.clickhouse.client.*;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.example.clickhousedemo.config.ClickHouseServer;
import com.example.clickhousedemo.service.ClickHouseService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
public class ClickhouseDemoApplication {

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(ClickhouseDemoApplication.class, args);
		ClickHouseService service = context.getBean(ClickHouseService.class);

		String table = "demo_table";
		// 기존 테이블이 있다면 테이블을 삭제 후 다시 생성합니다.
		service.dropAndCreateTable(table);

	}

	static long insert(ClickHouseNode server, String table) throws ClickHouseException {
		try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
			ClickHouseRequest.Mutation request = client.read(server).write().table(table)
				.format(ClickHouseFormat.RowBinary);
			ClickHouseConfig config = request.getConfig();
			CompletableFuture<ClickHouseResponse> future;
			// back-pressuring is not supported, you can adjust the first two arguments
			try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
				.createPipedOutputStream(config, (Runnable) null)) {
				// in async mode, which is default, execution happens in a worker thread
				future = request.data(stream.getInputStream()).execute();

				// writing happens in main thread
				for (int i = 0; i < 10_000; i++) {
					BinaryStreamUtils.writeString(stream, String.valueOf(i % 16));
					BinaryStreamUtils.writeNonNull(stream);
					BinaryStreamUtils.writeString(stream, UUID.randomUUID().toString());
				}
			}

			// response should be always closed
			try (ClickHouseResponse response = future.get()) {
				ClickHouseResponseSummary summary = response.getSummary();
				return summary.getWrittenRows();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw ClickHouseException.forCancellation(e, server);
		} catch (ExecutionException | IOException e) {
			throw ClickHouseException.of(e, server);
		}
	}

	static void dropAndCreateTable(ClickHouseNode server, String table) throws ClickHouseException {
		try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
			ClickHouseRequest<?> request = client.read(server);
			// or use future chaining
			request.query("drop table if exists " + table).execute().get();
			request.query("create table " + table + "(a String, b Nullable(String)) engine=MergeTree() order by a")
				.execute().get();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw ClickHouseException.forCancellation(e, server);
		} catch (ExecutionException e) {
			throw ClickHouseException.of(e, server);
		}
	}

	static int query(ClickHouseNode server, String table) throws ClickHouseException {
		try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
			 ClickHouseResponse response = client.read(server)
				 // prefer to use RowBinaryWithNamesAndTypes as it's fully supported
				 // see details at https://github.com/ClickHouse/clickhouse-java/issues/928
				 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
				 .query("select * from " + table).execute().get()) {
			int count = 0;
			// or use stream API via response.stream()
			for (ClickHouseRecord r : response.records()) {
				count++;
			}
			return count;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw ClickHouseException.forCancellation(e, server);
		} catch (ExecutionException e) {
			throw ClickHouseException.of(e, server);
		}
	}

}
