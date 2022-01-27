package stu.cfl.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.sun.javafx.scene.traversal.TopMostTraversalEngine;
import com.sun.xml.internal.bind.v2.TODO;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import stu.cfl.utils.KafkaUtil;

public class SplitLogApp {
    /**
     * 分流日志数据：页面数据、启动日志、曝光数据
     */
    public static void main(String[] args) {
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

        // TODO: 消费ODS_BASE_LOG数据
        String topic = "ODS_BASE_LOG";
        String groupId = "SplitLogApp";
        DataStreamSource<String> log = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO: 将每行数据转化为JSON对象（脏数据放入侧输出流）
        OutputTag<String> outPutTag = new OutputTag<String>("dirtyDataStream");

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = log.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(outPutTag, value);
                }
            }

        });

        // TODO: 验证新老用户（状态编程）
        // 按照mid分组 KeyedStream<T, KEY>
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> richMapStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> midState;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.midState = getRuntimeContext().getState(new ValueStateDescriptor<String>("midState", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String is_new = value.getJSONObject("common").getString("is_new");
                if (is_new.equals("1")) {
                    if (this.midState.value() != null) {
                        value.getJSONObject("common").put("is_new", 0);  // is_new为错误信息
                    } else {
                        this.midState.update("1");  // 新用户
                    }
                }
                return value;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

        });

        // TODO: 分流

        // TODO: 提取侧输出流

        // TODO: 将不同流存入kafka的不同主题中





    }
}
