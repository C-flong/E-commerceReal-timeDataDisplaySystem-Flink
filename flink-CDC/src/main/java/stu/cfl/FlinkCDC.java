package stu.cfl;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {
        // 1、获取Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启checkpoint并指定状态后端FS（memory， FS， rocksdb）
        env.setStateBackend(new FsStateBackend("hdfs://flink101:8020/realTimeSystem-Flink"));

        // 开启ck，两个ck执行间隔时间为5s
        env.enableCheckpointing(5000L);  // ms

        // 设置ck模式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 设置ck延迟时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        // 设置最大并存的ck数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        // 设置两ck最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);

        // 重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));

        // 2、通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()  // <T> Builder<T>返回是一个泛型类因此要加一个泛型
                .hostname("flink101")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("E-commerceReal-timeDataDisplaySystem-Flink")
                .tableList("E-commerceReal-timeDataDisplaySystem-Flink.base_trademark")  // 格式：db.tb，防止多数据库中包含同名表
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.latest())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 3、打印数据
        streamSource.print();

        // 4、启动任务
        env.execute("FlinkCDC");

    }



}
