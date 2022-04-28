package stu.cfl.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Component;
import stu.cfl.common.DBConfig;
import stu.cfl.service.ScreenService;
import stu.cfl.utils.JDBCUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

@Component
public class ScreenServiceImpl implements ScreenService {

    private Connection connection = null;
//    private Connection connection_mysql = null;
    private JSONArray citiesInfo = null;


    public ScreenServiceImpl() {
        try {
            Class.forName(DBConfig.CLICKHOUSE_DRIVER);
            this.connection = DriverManager.getConnection(DBConfig.CLICKHOUSE_URL);

//            Class.forName(DBConfig.MYSQL_DRIVER);
//            this.connection_mysql = DriverManager.getConnection(DBConfig.MYSQL_URL, "root", "1234");

            // 加载省份信息
            try {
                Path path = Paths.get("data-server/src/main/resources/cities.json");
                byte[] bytes = Files.readAllBytes(path);
                String s = new String(bytes);
                this.citiesInfo = JSONObject.parseArray(s);
            } catch (IOException e) {
                // TODO: 提供测试使用
                Path path = Paths.get("src/main/resources/cities.json");
                byte[] bytes = new byte[0];
                try {
                    bytes = Files.readAllBytes(path);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                String s = new String(bytes);
                this.citiesInfo = JSONObject.parseArray(s);

                e.printStackTrace();

            }

        } catch (ClassNotFoundException e) {
            System.out.println("获取驱动失败！！！");
        } catch (SQLException e) {
            System.out.println("获取连接失败！！！");
        }
    }

    @Override
    public BigDecimal queryOrderAmount() {
        /**
         * 总销售额
         * 翻牌器
         */
        try {
            String sql = "select sum(order_amount) as total from product_stats;";
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            return (BigDecimal) query.get(0).get("total");

        } catch (SQLException | InvocationTargetException | IllegalAccessException | InstantiationException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    @Override
    public List<JSONObject> queryTMName() {
        /**
         * 品牌排行
         * 4 水平柱状图
         */
        try {

            String sql = "select tm_id id, tm_name name, sum(order_amount) value from product_stats group by tm_id,tm_name having order_amount>0 order by value desc;";
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            // [ {"name":"上海","value":310}, ... ]
            ArrayList<JSONObject> res = new ArrayList<>();
            for (JSONObject jsonObject: query){
                JSONObject jo = new JSONObject();
                jo.put("name", jsonObject.get("name"));
                jo.put("value", (BigDecimal) jsonObject.get("value"));
                res.add(jo);
            }
            return res;

        } catch (SQLException | InvocationTargetException | IllegalAccessException | InstantiationException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    @Override
    public List<JSONObject> queryCategory() {
        /**
         * 品类中各品牌的具体商品占比
         * 3 饼图
         * [
         *        {
         * 		"children":[
         *            {
         * 				"children":[
         *                    {
         * 						"name": sku,
         * 						"value": orderAmount
         *                    },
         * 					...
         * 				],
         * 				"name": spu
         *            },
         * 			...
         * 		],
         * 		"name": category3
         *    },
         * 	...
         *
         * ]
         */

        String category3_sql = "select " +
                "category3_id, category3_name " +
                "from product_stats " +
                "group by category3_id, category3_name;";

        ArrayList<JSONObject> result = new ArrayList<>();

        try {
            List<JSONObject> categories = JDBCUtil.query(connection, category3_sql);
            for (JSONObject category3_jo: categories){
                Integer category3_id = category3_jo.getInteger("category3_id");
                String category3_name = category3_jo.getString("category3_name");

                JSONObject category3_child_jo = new JSONObject();
                ArrayList<JSONObject> spu_child_list = new ArrayList<>();

                String spu_sql = "select " +
                                "    spu_id, spu_name, sum(order_ct) order_amount " +
                                "from " +
                                "    product_stats " +
                                "where " +
                                "    `category3_id`=" + category3_id + " group by spu_id, spu_name;";

                List<JSONObject> spu_query = JDBCUtil.query(connection, spu_sql);

                for (JSONObject spu_jo: spu_query){
                    Integer spu_id = spu_jo.getInteger("spu_id");
                    String spu_name = spu_jo.getString("spu_name");
                    Double order_amount_spu = spu_jo.getDouble("order_amount");

                    JSONObject spu_child_jo = new JSONObject();
                    ArrayList<JSONObject> sku_child_list = new ArrayList<>();

                    String sku_sql = "select " +
                            "    sku_id, sku_name, sum(order_ct) order_amount " +
                            "from " +
                            "    product_stats " +
                            "where " +
                            "    category3_id=" + category3_id + " and spu_id=" + spu_id +
                            " group by sku_id, sku_name;";

                    List<JSONObject> sku_query = JDBCUtil.query(connection, sku_sql);
                    for (JSONObject sku_jo: sku_query){
                        Integer sku_id = sku_jo.getInteger("sku_id");
                        String sku_name = sku_jo.getString("sku_name");
                        Integer order_amount = sku_jo.getInteger("order_amount");

                        JSONObject sku_child_jo = new JSONObject();
                        sku_child_jo.put("sku_name", sku_name);
                        sku_child_jo.put("value", order_amount);

                        sku_child_list.add(sku_child_jo);
                    }

                    spu_child_jo.put("children", sku_child_list);
                    spu_child_jo.put("name", spu_name);
                    spu_child_jo.put("value", order_amount_spu);

                    spu_child_list.add(spu_child_jo);

                }

                // 添加一个children
                category3_child_jo.put("children", spu_child_list);
                category3_child_jo.put("name", category3_name);

                // 将品类放入结果中
                result.add(category3_child_jo);
            }
            return result;
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }
        return null;
    }

    @Override
    public List<JSONObject> querySPU() {
        /**
         * 商品spu的好评占比
         * 6 轮播饼图
         */
        String sql = "select \n" +
                    "    spu_name name, sum(comment_ct) stock, sum(good_comment_ct) sales \n" +
                    "from \n" +
                    "    product_stats \n" +
                    "group by \n" +
                    "    spu_id,spu_name;";

        try {
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            for (int i = 0; i < query.size(); i++){
                if (query.get(i).get("name").toString().length() > 15){
                    query.get(i).put("name", query.get(i).get("name").toString().split(" ")[0]);
                }
            }
            return query;
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }

    @Override
    public List<JSONObject> queryProvince() {
        /**
         * 省份信息
         * 2 地图
         */
        // 查询clickhouse数据源中的数据
        String sql_ch = "select " +
                "    province_name pname, sum(order_amount) order_amount " +
                "from " +
                "    province_stats " +
                "group by " +
                "    province_id, province_name " +
                "ORDER BY order_amount;";
        int index = 0;
        try {
            // 查询出clickhouse数据源中的数据
            ArrayList<JSONObject> result = new ArrayList<>();
            List<JSONObject> query = JDBCUtil.query(connection, sql_ch);
//            query.sort(new Comparator<JSONObject>() {
//                @Override
//                public int compare(JSONObject o1, JSONObject o2) {
//                    return o1.getDouble("order_amount") > o2.getDouble("order_amount") ? 0 : 1;
//                }
//            });

            JSONArray cities_child_list = new JSONArray();

            for (JSONObject jo: query){

                JSONObject provinceInfo = new JSONObject();

                String pname = jo.get("pname").toString();

                // 查询省份信息

                for (Object ob: citiesInfo){

                    JSONObject province = JSONObject.parseObject(ob.toString());

                    if (province.getString("pname").startsWith(pname)){

                        for (Object city_jo: province.getJSONArray("cities")){
                            JSONObject city_child_jo = new JSONObject();
                            JSONObject city = JSONObject.parseObject(city_jo.toString());

                            ArrayList<Double> doubles = new ArrayList<>();
                            doubles.add(city.getDouble("lng"));
                            doubles.add(city.getDouble("lat"));

                            city_child_jo.put("name", city.getString("cname"));
                            city_child_jo.put("value", doubles);

                            cities_child_list.add(city_child_jo);
                        }

                        index += 1;
                        break;
                    }

                }

                // 判断省份数据哪个级别的

                if (index == 20){
                    Collections.shuffle(cities_child_list);
                    List<Object> objects = cities_child_list.subList(0, cities_child_list.size() / 3);
                    provinceInfo.put("children", objects);
                    provinceInfo.put("name", "0%-60%");
                    result.add(provinceInfo);
                    cities_child_list = new JSONArray();
                }else if (index == 30){
                    Collections.shuffle(cities_child_list);
                    List<Object> objects = cities_child_list.subList(0, cities_child_list.size() / 3);
                    provinceInfo.put("children", objects);
                    provinceInfo.put("name", "60%-30%");
                    result.add(provinceInfo);
                    cities_child_list = new JSONArray();
                }else if (index == 34){
                    Collections.shuffle(cities_child_list);
                    List<Object> objects = cities_child_list.subList(0, cities_child_list.size() / 3);
                    provinceInfo.put("children", objects);
                    provinceInfo.put("name", "30%-100%");
                    result.add(provinceInfo);
                }

            }
            return result;

        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }

    @Override
    public List<JSONObject> queryVisitors() {
        /**
         * 新老用户的唯一访客数、页面访问数、进入页面数、跳出页面数、持续访问时间信息
         * 表格
         */

        String sql = "select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct, sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
                "from visitor_stats " +
                "group by is_new;";
        try {
            List<JSONObject> visitorInfo = JDBCUtil.query(connection, sql);
            ArrayList<JSONObject> res = new ArrayList<>();
            JSONObject jo_uv = new JSONObject();  // 唯一访客对比
            jo_uv.put("item", "唯一访客数");
            jo_uv.put("is_old", visitorInfo.get(0).getIntValue("uv_ct"));
            jo_uv.put("is_new", visitorInfo.get(1).getIntValue("uv_ct"));

            JSONObject jo_pv = new JSONObject();  // 页面访问对比
            jo_pv.put("item", "页面访问数");
            jo_pv.put("is_old", visitorInfo.get(0).getIntValue("pv_ct"));
            jo_pv.put("is_new", visitorInfo.get(1).getIntValue("pv_ct"));

            JSONObject jo_sv = new JSONObject();  // 进入页面对比
            jo_sv.put("item", "入口页面数");
            jo_sv.put("is_old", visitorInfo.get(0).getIntValue("sv_ct"));
            jo_sv.put("is_new", visitorInfo.get(1).getIntValue("sv_ct"));

            JSONObject jo_uj = new JSONObject();  // 跳出页面对比
            jo_uj.put("item", "跳出次数");
            jo_uj.put("is_old", visitorInfo.get(0).getIntValue("uj_ct"));
            jo_uj.put("is_new", visitorInfo.get(1).getIntValue("uj_ct"));

            JSONObject jo_dur = new JSONObject();  // 持续访问时间对比
            jo_dur.put("item", "持续访问时间");
            jo_dur.put("is_old", visitorInfo.get(0).getIntValue("dur_sum"));
            jo_dur.put("is_new", visitorInfo.get(1).getIntValue("dur_sum"));

            res.add(jo_uv);
            res.add(jo_pv);
            res.add(jo_sv);
            res.add(jo_uj);
            res.add(jo_dur);

            return res;
        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }

    @Override
    public List<JSONObject> queryTraffic() {
        /**
         * 分钟流量信息(同时访问人数)
         * 5
         * [
         *     {
         *         "name":"上海",
         *         "value":310
         *     },
         * 	...
         * ]
         */

        String sql = "SELECT \n" +
                "    sum(visitor_stats.uv_ct) AS uc, \n" +
                "    toDate(edt) date, \n" +
                "    toHour(edt) hour , \n" +
                "    toMinute(edt) minute \n" +
                "FROM visitor_stats\n" +
                "GROUP BY \n" +
                "    toDate(edt), \n" +
                "    toHour(edt), \n" +
                "    toMinute(edt)\n" +
                "ORDER BY \n" +
                "    toDate(edt) ASC, \n" +
                "    toHour(edt) ASC, \n" +
                "    toMinute(edt) ASC";

        try {
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            ArrayList<JSONObject> res = new ArrayList<>();
            for (JSONObject jo : query) {
                JSONObject kv = new JSONObject();
                kv.put("name", jo.getString("date") + ' ' + jo.getString("hour") + ':' + jo.getString("minute"));
                kv.put("value", jo.getInteger("uc"));
                res.add(kv);
            }
            return res;

        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }

    @Override
    public List<JSONObject> queryKeyWords() {
        /**
         *
         *
         * [
         *     {
         *         "name":"上海",
         *         "value":310
         *     },
         * 	...
         * ]
         */

        String sql = "SELECT \n" +
                "    keyword, \n" +
                "    sum(keyword_stats.ct * multiIf(source = 'SEARCH', 10, source = 'ORDER', 3, source = 'CART', 2, source = 'CLICK', 1, 0)) AS ct\n" +
                "FROM keyword_stats\n" +
                "GROUP BY keyword\n" +
                "ORDER BY sum(keyword_stats.ct) DESC";

        try {
            List<JSONObject> query = JDBCUtil.query(connection, sql);
            ArrayList<JSONObject> res = new ArrayList<>();
            for (JSONObject jo : query) {
                JSONObject kv = new JSONObject();
                kv.put("name", jo.getString("keyword"));
                kv.put("value", jo.getInteger("ct"));
                res.add(kv);
            }
            return res;

        } catch (SQLException | InstantiationException | IllegalAccessException | InvocationTargetException sqlException) {
            sqlException.printStackTrace();
        }

        return null;
    }


}
