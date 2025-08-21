package com.nomura.flinkCDC;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamSQL {
    public static void main(String[] args) {
        //1创建 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 2. 创建表
        tableEnv.executeSql(
                "CREATE TABLE t1 (\n" +
                        "  id STRING PRIMARY KEY NOT ENFORCED,\n" +
                        "  name STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = 'localhost',\n" +
                        "  'port' = '3306',\n" +
                        "  'username' = 'root',\n" +
                        "  'password' = 'root1234',\n" +
                        "  'database-name' = 'test',\n" +
                        "  'table-name' = 't1',\n" +
                        "  'server-time-zone' = 'UTC'\n" +
                        ")"
        );
        Table tableResult = tableEnv.sqlQuery("select * from t1");

        tableResult.execute().print();

    }
}
