package com.nomura.flinkCDC;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream {
    public static void main(String[] args) throws Exception {
        //1获取flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//取决与数据量
        //2开启checkpoint

        //3flinkCDC构建mysqlsource
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("root1234")//数据库信息
                .databaseList("test")
                .tableList("test.t1")//不设置，表示全监控，设置需要写库名.表明
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .serverTimeZone("UTC")
                .build();
        //4读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "Mysql Source");

        //5打印
        mysqlDS.print();

        //6启动程序
        env.execute();
    }
}
