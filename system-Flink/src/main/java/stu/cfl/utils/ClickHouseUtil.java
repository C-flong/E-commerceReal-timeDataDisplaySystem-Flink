package stu.cfl.utils;


import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import stu.cfl.bean.TransientSink;
import stu.cfl.common.DBConfig;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getSink(String sql){

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        // 反射获得类的属性（包括私有属性）
                        Field[] fields = t.getClass().getDeclaredFields();

                        // 偏移值，去除bean中非表中的字段
                        int offset = 0;

                        for (int i = 0; i < fields.length; i++){
                            try {
                                // 获取字段
                                Field field = fields[i];
                                // 设置私有属性允许访问
                                field.setAccessible(true);
                                // 判断是否为忽略字段的注解
                                if (field.getAnnotation(TransientSink.class) == null){
                                    offset++;
                                    continue;
                                }
                                // 获取值
                                Object value = field.get(t);
                                // 设置句柄preparedStatement参数
                                preparedStatement.setObject(i+1-offset, value);

                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                                System.out.println("对象参数获取失败");
                            }
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)  // 批量提交，5条提交一次
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(DBConfig.CLICKHOUSE_DRIVER)
                        .withUrl(DBConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}
