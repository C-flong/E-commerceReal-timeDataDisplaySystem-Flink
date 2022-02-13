package stu.cfl.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import stu.cfl.common.DBConfig;
import stu.cfl.utils.DimUtil;
import stu.cfl.utils.ThreadPoolUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // JDBC连接
        Class.forName(DBConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(DBConfig.PHOENIX_SERVER);

        // 连接池连接
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    String id = getId(input);
                    // 查询维度信息
                    JSONObject dimInfo = DimUtil.getDIMInfo(connection, tableName, id);
                    // 补充OrderWide
                    if (dimInfo != null){
                        appendInfo(input, dimInfo);
                    }
                    // 将数据写入结果中
                    resultFuture.complete(Collections.singletonList(input));

                } catch (SQLException | InvocationTargetException | IllegalAccessException | InstantiationException sqlException) {
                    sqlException.printStackTrace();
                } catch (ParseException e) {
                    System.out.println("appendInfo添加信息失败");
                    e.printStackTrace();
                }
            }
        });
    }

    protected abstract void appendInfo(T input, JSONObject dimInfo) throws ParseException;

    public abstract String getId(T input);

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        super.timeout(input, resultFuture);
    }
}
