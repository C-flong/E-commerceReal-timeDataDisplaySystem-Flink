package stu.cfl.app.dws;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import stu.cfl.app.function.KeywordUDTF;
import stu.cfl.bean.KeywordStats;
import stu.cfl.common.Constant;
import stu.cfl.utils.ClickHouseUtil;
import stu.cfl.utils.KafkaUtil;

public class KeywordsStatSQLApp {
    /**
     * 词频统计
     */
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

        // TODO 读取kafka数据--DDL
        String groupId = "KeywordsStatSQLApp";
        String pageViewSourceTopic ="DWD_PAGE_LOG";

        String sql1 = "CREATE TABLE page_view(" +
                "common MAP<STRING,STRING>," +
                "page MAP<STRING,STRING>," +
                "ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND" +
                ") " +
                "WITH (" +
                KafkaUtil.getKafkaDDL(pageViewSourceTopic,groupId) +
                ")";

        tableEnv.executeSql(sql1);

        // TODO 过滤 上一跳页面为“search” && 搜索词！=null
        String sql2 = "select " +
                    "page['item'] full_word," +
                    "rt" +
                "from page_view" +
                "where " +
                    "page['page_id']='good_list' and " +
                    "page['item'] IS NOT NULL ";
        Table tableResult = tableEnv.sqlQuery(sql2);

        // TODO 注册UDTF，分词、炸裂
        tableEnv.createTemporarySystemFunction("splitWord", KeywordUDTF.class);
        Table wordTable = tableEnv.sqlQuery("select " +
                "word, rt from " +
                tableResult +  // 还可创建一个视图
                " ," +
                " LATERAL TABLE(splitWord(full_word))");

        // TODO 分组，开窗，聚合
        String sql3 = "select " +
                "word keyword," +
                "count(*) ct, " +
                "'" + Constant.KEYWORD_SEARCH + "' source ," +  // 关键词来源（搜索search）
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from  "+
                wordTable +
                " GROUP BY TUMBLE(rt, INTERVAL '10' SECOND ), word";
        Table result = tableEnv.sqlQuery(sql3);

        // TODO 表转流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(result, KeywordStats.class);

        // TODO 写入ClickHouse
        keywordStatsDataStream.print();
        String sql = "insert into keyword_stats(keyword,ct,source,stt,edt,ts) " + " values(?,?,?,?,?,?)";
        keywordStatsDataStream.<KeywordStats>addSink(ClickHouseUtil.getSink(sql));

        // TODO 执行
        env.execute("KeywordsStatSQLApp");

    }
}
