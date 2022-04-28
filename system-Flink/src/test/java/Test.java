import com.alibaba.fastjson.JSONObject;
import stu.cfl.common.DBConfig;
import stu.cfl.utils.JDBCUtil;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Date;
import java.util.Random;

public class Test {
    @org.junit.Test
    public void testPhoenix() throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            Class.forName(DBConfig.PHOENIX_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("导包错误");
        }
        try {
            connection = DriverManager.getConnection(DBConfig.PHOENIX_SERVER,"","");
        } catch (SQLException sqlException) {
            System.out.println("连接错误");
        }
        String sql = "select * from REALTIMESYSTEM.DIM_USER_INFO where id='23'";
        List<JSONObject> query = JDBCUtil.query(connection, sql, JSONObject.class, false);
        System.out.println(query);
//        try {
//            stmt = connection.createStatement();
//        } catch (SQLException sqlException) {
//            System.out.println("创建句柄错误");
//        }
//        try {
//            rs = stmt.executeQuery(sql);
//        } catch (SQLException sqlException) {
//            System.out.println("执行错误");
//        }
//        while (rs.next()) {
//
//            System.out.println(rs);
//        }


//        if (rs != null) {
//            rs.close();
//        }
//
//        if (stmt != null) {
//            stmt.close();
//        }
//
//        if (connection != null) {
//            connection.close();
//        }
    }

    @org.junit.Test
    public void testDate(){

        String dateNow = new SimpleDateFormat("yyyy-MM-dd").format(new Date());

        System.out.println("当前时间为："+dateNow);
    }

    @org.junit.Test
    public void testRandom(){
        Random random = new Random();

        int i = random.nextInt(10);
        System.out.println(i);
    }

    public static void main(String[] args){
        return;
    }
}
