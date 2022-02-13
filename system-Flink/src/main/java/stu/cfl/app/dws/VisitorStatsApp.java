package stu.cfl.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;
import stu.cfl.bean.VisitorStats;
import stu.cfl.utils.ClickHouseUtil;
import stu.cfl.utils.DateTimeUtil;
import stu.cfl.utils.KafkaUtil;

import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    /**
     * 访客主题宽表计算访客主题宽表计算
     */

    public static void main(String[] args) throws Exception {

        // TODO: 环境
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


        // TODO: 读取kafka数据
        String groupId = "VisitorStatsApp";
        String pageViewSourceTopic = "DWD_PAGE_LOG";
        String uniqueVisitSourceTopic = "DWM_UNIQUE_VISIT";
        String userJumpDetailSourceTopic = "DWM_USER_JUMP_DETAIL";
        DataStreamSource<String> uvDS = env.addSource(KafkaUtil.getKafkaConsumer(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> ujDS = env.addSource(KafkaUtil.getKafkaConsumer(userJumpDetailSourceTopic, groupId));
        DataStreamSource<String> pvDS = env.addSource(KafkaUtil.getKafkaConsumer(pageViewSourceTopic, groupId));


        // TODO: 将每个流处理成相同的数据类型
        // 处理uv数据
        SingleOutputStreamOperator<VisitorStats> uvMapDS = uvDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L, 0L, 0L, 0L, 0L,
                    jsonObject.getLong("ts")
            );
        });

        // 处理uj数据
        SingleOutputStreamOperator<VisitorStats> ujMapDS = ujDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 0L, 0L, 1L, 0L,
                    jsonObject.getLong("ts")
            );
        });
        
        // 处理pv数据
        SingleOutputStreamOperator<VisitorStats> pvMapDS = pvDS.map(data -> {
            JSONObject jsonObject = JSON.parseObject(data);
            JSONObject common = jsonObject.getJSONObject("common");
            JSONObject page = jsonObject.getJSONObject("page");
            String last_page_id = page.getString("last_page_id");
            long sv = 0L;
            if (last_page_id == null || last_page_id.length() <= 0){
                // 进入页面判断数据，上一条为空则为1
                sv = 1L;
            }
            return new VisitorStats(
                    "", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, 1L, sv, 1L, page.getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });


        // TODO: Union
        DataStream<VisitorStats> unionDS = uvMapDS.union(ujMapDS, pvMapDS);


        // TODO: 提取时间戳生成watermark
        SingleOutputStreamOperator<VisitorStats> unionDSWithWM = unionDS.assignTimestampsAndWatermarks(
                // 水位线延迟时间要大于10s，由于uj中的cep模式窗口为10秒，来的比较慢，因此这边需要较大的延迟时间
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO: 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = unionDSWithWM.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<String, String, String, String>(
                        value.getAr(),
                        value.getCh(),
                        value.getIs_new(),
                        value.getVc()
                );
            }
        });

        // TODO: 开窗聚合 10s的滚动窗口（轻度聚合）（增量聚合 ReduceFunction + 全量集合 WindowFunction, 一个窗户只输出一条）
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;

                    }
                },
                new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> key, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        /**
                         * key – The key for which this window is evaluated.
                         * window – The window that is being evaluated.
                         * input – The elements in the window being evaluated.
                         * out – A collector for emitting elements.
                         */
                        long start = window.getStart();
                        long end = window.getEnd();
                        VisitorStats visitorStats = input.iterator().next();  // 只有一条数据，故直接next
                        visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(start)));
                        visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(end)));
                        out.collect(visitorStats);
                    }
                }
        );

        // TODO: 将数据写入clickhouse
        reduceDS.print();
        String sql = "insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)";
        reduceDS.addSink(ClickHouseUtil.getSink(sql));

        // TODO: 执行
        env.execute("VisitorStatsApp");

    }


}
