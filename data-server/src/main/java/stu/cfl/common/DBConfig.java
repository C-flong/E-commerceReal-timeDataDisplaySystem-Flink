package stu.cfl.common;

public class DBConfig {

    // ClickHouse连接参数
    // ClickHouse url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://flink101:8123/default";

    // ClickHouse driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // Mysql连接参数
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://flink101:3306/E-commerceReal-timeDataDisplaySystem-Flink ";


}
