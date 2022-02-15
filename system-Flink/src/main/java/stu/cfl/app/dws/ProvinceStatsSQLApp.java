package stu.cfl.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import stu.cfl.bean.ProvinceStats;
import stu.cfl.utils.ClickHouseUtil;
import stu.cfl.utils.KafkaUtil;
/**
 * 地区主题宽表
 * 地区主题主要是反映各个地区的销售情况
 */
public class ProvinceStatsSQLApp {

    public static void main(String[] args) throws Exception {
        // TODO 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        env.setStateBackend(new FsStateBackend("hdfs://flink101:8020/realTimeSystem-Flink"));
        env.enableCheckpointing(5000);
        // 设置ck模式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置ck延迟时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 设置最大并存的ck数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置两ck最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 重启策略
        //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        */
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 使用DDL创建表 提取时间戳生成WaterMark
        String groupId = "ProvinceStatsSQLApp";
        String orderWideTopic = "DWM_ORDER_WIDE";

        String sql1 = "CREATE TABLE order_wide (" +
                "province_id BIGINT, " +
                "province_name STRING, " +
                "province_area_code STRING, " +
                "province_iso_code STRING, " +
                "province_3166_2_code STRING, " +
                "order_id STRING, " +
                "total_amount DOUBLE, " +
                "create_time STRING, " +
                // 创建水位线
                "rowtime AS TO_TIMESTAMP(create_time), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND)" +
                " WITH (" +
                KafkaUtil.getKafkaDDL(orderWideTopic, groupId) +
                ")";

        tableEnv.executeSql(sql1);

        // TODO 查询（分组，开窗，聚合）
        String sql2 = "select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                "province_id, " +
                "province_name, " +
                "province_area_code, " +
                "province_iso_code, " +
                "province_3166_2_code, " +
                "count( DISTINCT order_id) order_count, " +
                "sum(total_amount) order_amount, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by " +
                    "TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                    "province_id," +
                    "province_name," +
                    "province_area_code," +
                    "province_iso_code," +
                    "province_3166_2_code";

        Table table = tableEnv.sqlQuery(sql2);

        // TODO 将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(table, ProvinceStats.class);

        // TODO 写入ClickHouse
        provinceStatsDS.print();
        String sql3 = "insert into	province_stats values(?,?,?,?,?,?,?,?,?,?)";
        provinceStatsDS.addSink(ClickHouseUtil.getSink(sql3));

        // TODO 执行
        env.execute("ProvinceStatsSQLApp");


    }
}
