package stu.cfl.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    /**
     * JDBC工具: 操作数据库通用工具类
     */

    public static List<JSONObject> query(
            Connection connection,
            String sql
    ) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
        /**
         * connection: 连接
         * sql: 查询
         * cls: 返回的对象类型
         * underScoreToCamel: 是否需要将下划线转化为驼峰的标志
         */
        ArrayList<JSONObject> res = new ArrayList<>();
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();

        // 解析查询返回值（由于T不一定就是bean对象，因此直接用JSON::parseObject不合适）
        ResultSetMetaData metaData = resultSet.getMetaData();  // 元数据信息
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()){
            JSONObject jsonObject = new JSONObject();
            // 封装对象t
            for (int i = 1; i <= columnCount; i++){
                // 列名
                String columnName = metaData.getColumnName(i);
                // 列值
                Object value = resultSet.getObject(i);
                jsonObject.put(columnName, value);
            }
            res.add(jsonObject);
        }

        // 关闭连接
        preparedStatement.close();
        resultSet.close();

        return res;
    }



}
