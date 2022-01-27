package stu.cfl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkCDCWithSQL {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 建表
//        https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html
        tableEnv.executeSql("CREATE TABLE testFSQL (" +
                "	id INT," +
                "	tm_name STRING," +
                "	logo_url STRING" +
                ") WITH (" +
                "	'connector' = 'mysql-cdc'," +
                "	'hostname' = 'flink101'," +
                "	'port' = '3306'," +
                "	'username' = 'root'," +
                "	'password' = '1234'," +
                "	'database-name' = 'E-commerceReal-timeDataDisplaySystem-Flink'," +
                "	'table-name' = 'base_trademark'" +
                ")");
        // 查询
//        tableEnv.executeSql("select * from testFSQL").print();
        Table table = tableEnv.sqlQuery("select * from testFSQL");

        // 将动态表转化为流
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute("FlinkCDCWithSQL");


    }
}
