package stu.cfl.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import stu.cfl.utils.KafkaUtil;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    /**
     * 跳出明细计算
     * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
     * 关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
     * @param args
     */
    public static void main(String[] args) throws Exception {
        // TODO: 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 一般并行度和kafka主题分区数保持一致
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
        // TODO: 读取kafka中dwm层的页面信息（DWD_PAGE_LOG）
        String sourceTopic = "DWD_PAGE_LOG";
        String groupId = "UserJumpDetailApp";
        String sinkTopic = "DWM_USER_JUMP_DETAIL";

        DataStreamSource<String> initialDS = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO: 解析成json，并提取ts生成watermark（不能单纯靠ts来分辨数据顺序，数据可能时乱序的因此要加上水位线来保证数据可靠性）
        SingleOutputStreamOperator<JSONObject> mapDS = initialDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> parsedDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(
                                Duration.ofSeconds(1)
                        ).withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }
                )
        );

        // TODO: 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;
                    }
                })
                .within(Time.seconds(10));
//        Pattern.<JSONObject>begin("start")
//                .where(new SimpleCondition<JSONObject>() {
//                    @Override
//                    public boolean filter(JSONObject value) throws Exception {
//                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
//                        return lastPageId == null || lastPageId.length() <= 0;
//                    }
//                })
//                .times(2)
//                .consecutive()
//                .within(Time.seconds(10));
        // TODO: 将模式序列作用在流上
        PatternStream<JSONObject> patternDS = CEP.pattern(
                // 应该将模式作用于不同用户，因此需要做分组
                parsedDS.keyBy(data -> data.getJSONObject("common").getString("mid")),
                pattern);

        // TODO: 提取匹配上的和超时事件
        OutputTag<JSONObject> timeoutTag = new OutputTag<JSONObject>("timeoutTag"){};
        SingleOutputStreamOperator<JSONObject> selectDS = patternDS.select(
                timeoutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("start").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("start").get(0);
                    }
                }
        );
        // TODO: UNION两种事件
        DataStream<JSONObject> timeoutOutputDS = selectDS.getSideOutput(timeoutTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeoutOutputDS);

        // TODO: 将数据写入kafka
        unionDS.print();
        unionDS.map(JSON::toString)
                .addSink(KafkaUtil.getFlinkKafkaProducer(sinkTopic));

        // TODO: 执行
        env.execute("UserJumpDetailApp");


    }
}
