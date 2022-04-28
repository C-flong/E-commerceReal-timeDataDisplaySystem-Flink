package stu.cfl.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import stu.cfl.utils.KafkaUtil;

public class SplitLogApp {
    /**
     * 分流日志数据：页面日志、启动日志、曝光日志
     * 页面日志输出到主流,启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
     */
    public static void main(String[] args) throws Exception {
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
        // 侧输出流对象声明 OutputTag<Tuple2<String, Long>> info = new OutputTag<Tuple2<String, Long>>("late-data"){};
        OutputTag<String> outPutTag = new OutputTag<String>("dirtyDataStream"){};

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

        // TODO: 验证新老访客（状态编程）
        // 按照mid（设备id）分组 KeyedStream<T, KEY>
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> richMapStream = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<Integer> midState;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.midState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("midState", Integer.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                String is_new = value.getJSONObject("common").getString("is_new");

                if ("1".equals(is_new)) {
                    if (this.midState.value() != null) {
                        if (this.midState.value() >= 2){
                            value.getJSONObject("common").put("is_new", "0");  // is_new为错误信息
                        }
                        else{
                            this.midState.update(2);
                        }
                    } else {
//                        System.out.println(value);
                        this.midState.update(1);  // 新用户
                    }
                }


                return value;
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

        });
//        richMapStream.writeAsText("D:\\WorkSpace\\IdeaProjects\\E-commerceReal-timeDataDisplaySystem-Flink\\system-Flink\\src\\main\\resources\\splitLog.json");
//        richMapStream.print();
        // TODO: 分流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        // 主流：页面日志
        SingleOutputStreamOperator<String> pageStream = richMapStream.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {
                JSONObject start = value.getJSONObject("start");
                if (start != null) {
                    // 属于启动日志,放入start侧输出流
                    ctx.output(startTag, value.toString());
                } else {
                    out.collect(value.toString());
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        // 是曝光数据,炸裂后将页面id存入每个数据中，再放入侧输出流
                        String page_id = value.getJSONObject("page").getString("page_id");
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject page = displays.getJSONObject(i);
                            page.put("page_id", page_id);
                            ctx.output(displayTag, page.toString());
                        }
                    }
                }
            }
        });

        // TODO: 提取侧输出流
        DataStream<String> startSideOutput = pageStream.getSideOutput(startTag);
        DataStream<String> displaySideOutput = pageStream.getSideOutput(displayTag);

        // TODO: 将不同流存入kafka的不同主题中
        pageStream.addSink(KafkaUtil.getFlinkKafkaProducer("DWD_PAGE_LOG"));
        startSideOutput.addSink(KafkaUtil.getFlinkKafkaProducer("DWD_START_LOG"));
        displaySideOutput.addSink(KafkaUtil.getFlinkKafkaProducer("DWD_DISPLAY_LOG"));

//        pageStream.print();
        // TODO: 执行
        env.execute("SplitLogApp");



    }
}
