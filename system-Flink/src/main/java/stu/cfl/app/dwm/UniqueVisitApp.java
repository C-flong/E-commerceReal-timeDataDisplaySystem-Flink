package stu.cfl.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.cfl.utils.KafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class UniqueVisitApp {
    /**
     * UV计算，可用于统计日活
     */
    public static void main(String[] args) throws Exception {
        // TODO: 环境设置
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

        // TODO: 读取kafka中页面记录主题（DWD_PAGE_LOG）
        String topic = "DWD_PAGE_LOG";
        String groupId = "UniqueVisitApp";
        String sinkTopic = "DWM_UNIQUE_VISIT";
        DataStreamSource<String> initialDS = env.addSource(KafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO: 转换未json类型
        SingleOutputStreamOperator<JSONObject> mapDS = initialDS.map(JSONObject::parseObject);

        // TODO: 过滤数据，过滤同一个用户一天内的重复数据---状态编程
        // value: {
        // "actions":[{"action_id":"get_coupon","item":"1","item_type":"coupon_id","ts":1611153684425}],
        // "common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone 8","mid":"mid_1","os":"iOS 13.3.1","uid":"13","vc":"v2.1.134"},
        // "displays":[{"display_type":"query","item":"8","item_type":"sku_id","order":1,"pos_id":5},{"display_type":"query","item":"10","item_type":"sku_id","order":2,"pos_id":5},{"display_type":"query","item":"2","item_type":"sku_id","order":3,"pos_id":2},{"display_type":"promotion","item":"3","item_type":"sku_id","order":4,"pos_id":5}],
        // "page":{"during_time":10851,"item":"8","item_type":"sku_id","last_page_id":"home","page_id":"good_detail","source_type":"promotion"},
        // "ts":1611153679000
        // }
        // data: {column1: "", column2: "", column3: "", ...}

        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
//        KeyedStream<JSONObject, String> keyedStream = mapDS.keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("common").getString("mid");
//            }
//        });
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("VSD", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(stringValueStateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() <= 0) {
                    // 当前用户某天访问
                    String date = valueState.value();
                    String curDate = simpleDateFormat.format(value.getLong("ts"));
                    if (!curDate.equals(date)) {
                        // 当天的首次登录
                        valueState.update(curDate);
                        return true;
                    }
                }
                return false;

            }
        });

        // TODO: 写入kafka
        filterDS.print();
        filterDS.map(JSON::toString)
                .addSink(KafkaUtil.getFlinkKafkaProducer(sinkTopic));


        // TODO: 执行
        env.execute("UniqueVisitApp");


    }


}
