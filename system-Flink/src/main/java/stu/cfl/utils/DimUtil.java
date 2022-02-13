package stu.cfl.utils;

import com.alibaba.fastjson.JSONObject;
import stu.cfl.common.DBConfig;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DimUtil {
    /**
     * 操作DIM层数据的工具类
     */

    public static JSONObject getDIMInfo(Connection connection, String tableName, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // 优化：借助redis做缓存 P99
        // ？？？

        // 通过
        String sql = "select * from "
                + DBConfig.HBASE_SCHEMA
                + "." + tableName
                + " where id="
                + "'"
                + id
                + "'";
        List<JSONObject> query = JDBCUtil.query(connection, sql, JSONObject.class, true);
        return query.get(0);
    }
}
