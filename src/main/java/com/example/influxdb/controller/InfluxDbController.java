package com.example.influxdb.controller;


import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxTable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lingjinli
 * @date 2024年11月12日 8:47
 * @desc 功能点
 */
@RestController
@RequestMapping("/influxdb")
public class InfluxDbController {

    @Autowired
    private InfluxDBClient influxDBClient;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Value("${influxdb.org}")
    private String org;

    // 写入数据到 InfluxDB
    @PostMapping("/write")
    public String writeData(@RequestParam String measurement, @RequestParam String location, @RequestParam Double value) {
        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            Point point = Point.measurement(measurement)      // 设置测量名称
                    .addTag("location", location)   // 添加标签
                    .addField("value", value)       // 添加字段
                    .time(Instant.now(), WritePrecision.NS);// 设置时间戳

            // 写入数据
            writeApi.writePoint(bucket, org, point);
            return "Data written successfully.";
        } catch (Exception e) {
            return "Error writing data: " + e.getMessage();
        }
    }

    // 查询数据
    @GetMapping("/query")
    public List<String> queryData(@RequestParam String measurement, @RequestParam String start, @RequestParam String stop) {
        QueryApi queryApi = influxDBClient.getQueryApi();
        // Flux 查询语句，使用 range 参数设置时间范围
        String flux = String.format("from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"%s\")", bucket, start, stop, measurement);
        List<FluxTable> tables = queryApi.query(flux, org);
        // 将查询结果转换为字符串格式返回
        return tables.stream().flatMap(table -> table.getRecords().stream())
//                todo 这里的valu需要使用_value
                .map(record -> String.format("Time: %s, Location: %s,Value:%s", record.getTime(), record.getValueByKey("location"), record.getValueByKey("_value"))).collect(Collectors.toList());
    }
}
