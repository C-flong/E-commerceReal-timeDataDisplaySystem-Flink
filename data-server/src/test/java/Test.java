import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import stu.cfl.common.DBConfig;
import stu.cfl.service.ScreenService;
import stu.cfl.service.impl.ScreenServiceImpl;
import stu.cfl.utils.JDBCUtil;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Test {


    @org.junit.Test
    public void testJDBCUtil_queryOrderAmount(){
        Connection connection = null;
        try {
            Class.forName(DBConfig.CLICKHOUSE_DRIVER);
            connection = DriverManager.getConnection(DBConfig.CLICKHOUSE_URL);
        } catch (ClassNotFoundException e) {
            System.out.println("获取驱动失败！！！");
        } catch (SQLException e) {
            System.out.println("获取连接失败！！！");
        }
        String sql = "select sum(order_amount) as total from product_stats;";
        try {
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            System.out.println(query);
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }
    }

    @org.junit.Test
    public void testJDBCUtil_queryTMName(){
        Connection connection = null;
        try {
            Class.forName(DBConfig.CLICKHOUSE_DRIVER);
            connection = DriverManager.getConnection(DBConfig.CLICKHOUSE_URL);
        } catch (ClassNotFoundException e) {
            System.out.println("获取驱动失败！！！");
        } catch (SQLException e) {
            System.out.println("获取连接失败！！！");
        }
        String sql = "select tm_id id, tm_name name, sum(order_amount) value from product_stats group by tm_id,tm_name having order_amount>0 order by value desc;";
        try {
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            System.out.println(query);
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }
    }

    @org.junit.Test
    public void testJDBCUtil_queryCategory() throws IOException {
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.queryCategory();
        FileOutputStream fileOutputStream = new FileOutputStream("src/main/resources/3.txt");
        fileOutputStream.write(jsonObjects.toString().getBytes());
        fileOutputStream.close();
    }

    @org.junit.Test
    public void testJDBCUtil_querySPU() throws IOException {
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.querySPU();
        FileOutputStream fileOutputStream = new FileOutputStream("src/main/resources/6.txt");
        fileOutputStream.write(jsonObjects.toString().getBytes());
        fileOutputStream.close();
    }

    @org.junit.Test
    public void testMysqlConnection() throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class.forName(DBConfig.MYSQL_DRIVER);
        Connection connection = DriverManager.getConnection(DBConfig.MYSQL_URL, "root", "1234");
        String sql = "SELECT id, name FROM sh_area where pid=0;";
        List<JSONObject> query = JDBCUtil.query(connection, sql);
        System.out.println(query);

    }

    @org.junit.Test
    public void readJsonInfo() throws IOException {
        Path path = Paths.get("src/main/resources/city.json");
        byte[] bytes = Files.readAllBytes(path);
        String s = new String(bytes);
        JSONObject jsonObject = JSONObject.parseObject(s);

        // [{pid: , {name: , lng:, lat: }}, ]
        ArrayList<JSONObject> result_list = new ArrayList<>();
        int pid = 0;
        int index = -1;
        for (Object jo: jsonObject.getJSONArray("RECORDS")){
            JSONObject res = new JSONObject();
            JSONObject jo_p = JSONObject.parseObject(jo.toString());
            JSONObject city = new JSONObject();
            if (jo_p.getInteger("pid") != pid){
                // 新的省份
                pid = jo_p.getInteger("pid");

                res.put("pid", pid);
                res.put("pname", jo_p.get("pname").toString());
                ArrayList<JSONObject> jsonObjects = new ArrayList<>();

                city.put("cname", jo_p.get("cname"));
                city.put("lng", jo_p.get("lng"));
                city.put("lat", jo_p.get("lat"));
                jsonObjects.add(city);
                res.put("cities", jsonObjects);
                result_list.add(res);
                index += 1;
            }
            else{
                // 已存在省份
                city.put("cname", jo_p.get("cname"));
                city.put("lng", jo_p.get("lng"));
                city.put("lat", jo_p.get("lat"));
                JSONArray cities = result_list.get(index).getJSONArray("cities");
                cities.add(city);
                result_list.get(index).put("cities", cities);
            }
        }

//        Path path1 = Paths.get("src/main/resources/cities.json");
//        Files.write(path1, result_list.toString().getBytes());
        System.out.println(result_list);
    }

    @org.junit.Test
    public void testQueryProvince() throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.queryProvince();
        System.out.println(jsonObjects);
    }

    @org.junit.Test
    public void test() throws ClassNotFoundException, SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        System.out.println(3/2);
    }

    @org.junit.Test
    public void testQueryVisitors(){
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.queryVisitors();
        System.out.println(jsonObjects);
    }

    @org.junit.Test
    public void testQueryTraffic(){
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.queryTraffic();
        System.out.println(jsonObjects);
    }

    @org.junit.Test
    public void testQueryKeyWords(){
        ScreenServiceImpl screenService = new ScreenServiceImpl();
        List<JSONObject> jsonObjects = screenService.queryKeyWords();
        System.out.println(jsonObjects);
    }
}
