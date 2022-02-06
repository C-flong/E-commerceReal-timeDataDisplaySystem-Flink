package stu.cfl.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import stu.cfl.app.function.CustomerDeserialization;
import stu.cfl.app.function.DimSinkFunction;
import stu.cfl.app.function.TableProcessFunction;
import stu.cfl.bean.TableProcess;
import stu.cfl.utils.KafkaUtil;

import javax.annotation.Nullable;

public class SplitDBApp {
    public static void main(String[] args) throws Exception {
        // TODO: 2022/1/28 获取环境
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
        // TODO: 2022/1/28 消费 ODS_BASE_LOG 主题
        String sourceTopic = "ODS_BASE_DB";
        String groupId = "SplitDBApp";
        DataStreamSource<String> originalStream = env.addSource(KafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO: 2022/1/28 将数据转化为json对象并过滤delete操作的数据
        SingleOutputStreamOperator<JSONObject> filterStream = originalStream.map(data -> JSON.parseObject(data))
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return !"delete".equals(value.getString("type"));
                    }
                });

        // TODO: 2022/1/28 使用FlinkCDC获取配置表并生成 广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("flink101")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("realtimeSystem")
                .tableList("realtimeSystem.table_process")
                .startupOptions(StartupOptions.latest())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> processTableStream = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>(
                "map_state",
                String.class,
                TableProcess.class
        );

        BroadcastStream<String> process = processTableStream.broadcast(mapStateDescriptor);

        // TODO: 2022/1/28 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterStream.connect(process);

        // TODO: 2022/1/28 分流
        OutputTag<JSONObject> tagHBase = new OutputTag<JSONObject>("tag_hbase") {};
        SingleOutputStreamOperator<JSONObject> kafkaDS = connectDS.process(
                new TableProcessFunction(
                        tagHBase,
                        mapStateDescriptor
                )
        );

        // TODO: 2022/1/28 输出至kafka和HBase
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(tagHBase);
        hbaseDS.addSink(new DimSinkFunction());
        // 传入至kafka中由于每条数据下沉的话题可能不一样，因此原先的工具类不能使用，应该根据每条记录中的sinkTable来定，因此要换一种下沉函数
        kafkaDS.addSink(KafkaUtil.getFlinkKafkaProducer(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        KafkaSerializationSchema.super.open(context);
                    }
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                        return new ProducerRecord<byte[], byte[]>(
                                element.getString("sinkTable"),
                                element.getString("after").getBytes()
                        );
                    }
                }
        ));

        // TODO: 2022/1/28 启动
        env.execute();
    }
}
