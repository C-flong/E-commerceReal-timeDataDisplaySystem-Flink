package stu.cfl.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import stu.cfl.bean.TableProcess;
import stu.cfl.common.DBConfig;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 合并主流和广播流，综合信息后并进行分流
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection connection;
    private OutputTag<JSONObject> tagHBase;
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;


    public TableProcessFunction(){}

    public TableProcessFunction(OutputTag<JSONObject> tag, MapStateDescriptor<String, TableProcess> descriptor){
        this.tagHBase = tag;
        this.mapStateDescriptor = descriptor;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(DBConfig.PHOENIX_DRIVER);
        this.connection = DriverManager.getConnection(DBConfig.PHOENIX_SERVER);
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        /**
         * 广播流
         * value: {db: "", tn: "", before: {}, after: {}, type: ""}
         * data: {sourceTable: "", operateType: "", sinkType: "", sinkTable: "", sinkColumns: "", sinkPk: "", sinkExtend: ""}
         */
        // 解析CDC数据(mysql中的realtimeSystem.table_process表)
        // {"database":"E-commerceReal-timeDataDisplaySystem-Flink",
        // "before":{},"after":{"tm_name":"12","logo_url":"12","id":12},
        // "type":"insert","tableName":"base_trademark"}
        JSONObject jsonObject = JSONObject.parseObject(value);
        // 删除table_process表数据的情况未考虑，目前先直接跳出
        if (!jsonObject.getString("type").equals("insert"))
            return;
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSONObject.parseObject(data, TableProcess.class);
        PreparedStatement preparedStatement = null;

        // 校验是否存在对应的HBase表 (目前仅考虑了增加表的情况，删除表待考虑)

        if(tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
            try{
                // 判断是否是写入hbase的数据
                String sinkTable = tableProcess.getSinkTable();
                String sinkColumns = tableProcess.getSinkColumns();
                String sinkPk = tableProcess.getSinkPk();
                if (sinkPk == null)
                    sinkPk = "id";
                String sinkExtend = tableProcess.getSinkExtend();
                if (sinkExtend == null)
                    sinkExtend = "";

                // 建表
                StringBuffer createSQL = new StringBuffer("create table if not exists ")
                        .append(DBConfig.HBASE_SCHEMA)
                        .append(".")
                        .append(sinkTable)
                        .append("(");
                String[] columns = sinkColumns.split(",");
                for(String s: columns){
                    // 判断是否为主键
                    if (sinkPk.equals(s)){
                        createSQL.append(s + " varchar primary key");
                    }
                    else{
                        createSQL.append(s + " varchar");
                    }
                    // 加逗号
                    createSQL.append(",");
                }
                // 删除最后一个多余的逗号
                createSQL.deleteCharAt(createSQL.length()-1);

                createSQL.append(") ").append(sinkExtend);
                // 打印建表语句
                System.out.println(createSQL);

                // 预编译SQL
                preparedStatement = connection.prepareStatement(createSQL.toString());
                preparedStatement.execute();
            }
            catch (SQLException se){
                se.printStackTrace();
                System.out.println("建表失败！！！");
            }
            finally {
                if (preparedStatement != null){
                    try{
                        preparedStatement.close();
                    }
                    catch (SQLException se){
                        se.printStackTrace();
                    }
                }
            }
        }
        // 写入状态，并广播
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(this.mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "," + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }


    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        /**
         * 主流
         * value: {db: "", tn: "", before: {}, after: {}, type: ""}
         * data: {column1: "", column2: "", column3: "", ...}
         */
        // 获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = value.getString("tableName") + "," + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);


        if (tableProcess != null){
            // 过滤字段
            String[] columnsArray = tableProcess.getSinkColumns().split(",");
            List<String> columnsList = Arrays.asList(columnsArray);
            JSONObject data = value.getJSONObject("after");
//            Set<Map.Entry<String, Object>> entries = data.entrySet();
//            for (Map.Entry<String, Object> entry : entries) {
//                if (!columnsList.contains(entry.getKey())) {
//                    entries.remove(entry);
//                }
//            }
            Set<Map.Entry<String, Object>> entries = data.entrySet();
            entries.removeIf(entry -> !columnsList.contains(entry.getKey()));

            // 分流（kafka|hbase）
            String sinkTable = tableProcess.getSinkTable();
            // 添加下沉去处
            value.put("sinkTable", sinkTable);
            String sinkType = tableProcess.getSinkType();
            if (sinkType.equals(TableProcess.SINK_TYPE_HBASE)){
                // 传入hbase的数据写出至侧输出流
                System.out.println("hbase---" + value);
                ctx.output(tagHBase, value);
            }
            else if (sinkType.equals(TableProcess.SINK_TYPE_KAFKA)){
                // 传入kafka的数据写出至侧主流
                System.out.println("kafka---" + value);
                out.collect(value);
            }

        }else{
            System.out.println("无法定位该记录的去向！！！");
        }




    }



}
