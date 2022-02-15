package stu.cfl.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import stu.cfl.common.DBConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * 自定义下沉函数下沉至HBase
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        /**
         * 启动时进行连接
         */
        Class.forName(DBConfig.PHOENIX_DRIVER);
        this.connection = DriverManager.getConnection(DBConfig.PHOENIX_SERVER);
    }


    @Override
    public void invoke(JSONObject value, Context context){
        /**
         * 执行存储操作
         * value: {db: "", tn: "", before: {}, after: {}, type: "", sinkTable: ""}
         * data: {column1: "", column2: "", column3: "", ...}
         */
        JSONObject data = value.getJSONObject("after");
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        // 制作sql
        String sql = "upsert into "
                + DBConfig.HBASE_SCHEMA
                + "."
                + value.getString("sinkTable")
                + " ("
                + StringUtils.join(keys, ",")
                + ") "
                + "values('"
                + StringUtils.join(values, "','")
                + "')";
        System.out.println(sql);
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = connection.prepareStatement(sql);
        } catch (SQLException sqlException) {
            System.out.println("SQL编译失败");
            sqlException.printStackTrace();
        }
        try {
            preparedStatement.executeUpdate();
        } catch (SQLException sqlException) {
            System.out.println("SQL执行失败");
            sqlException.printStackTrace();
        }
        try {
            connection.commit();
        } catch (SQLException sqlException) {
            System.out.println("提交失败");
            sqlException.printStackTrace();
        }
        try {
            preparedStatement.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

    }
}
