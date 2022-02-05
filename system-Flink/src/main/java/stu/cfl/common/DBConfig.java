package stu.cfl.common;
import org.apache.phoenix.jdbc.PhoenixDriver;
public class DBConfig {
    // HBase库名
    public static final String HBASE_SCHEMA = "realtimeSystem";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:flink101,flink102,flink103:2181";

}
