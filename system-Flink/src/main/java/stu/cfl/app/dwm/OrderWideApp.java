package stu.cfl.app.dwm;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import stu.cfl.app.function.DimAsyncFunction;
import stu.cfl.bean.OrderDetail;
import stu.cfl.bean.OrderInfo;
import stu.cfl.bean.OrderWide;
import stu.cfl.utils.KafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * 订单宽表，订单信息加细节并关联用户、地区、商品、品类、品牌
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {

        // TODO: 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /*
        env.setStateBackend(new FsStateBackend("hdfs://flink101:8020/realTimeSystem-Flink"));
        env.enableCheckpointing(5000);
        // 设置ck模式为exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置ck延迟时间
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        // 设置最大并存的ck数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 设置两ck最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        // 重启策略
        //  env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        */

        // TODO: 从kafka中获取数据 ---> 转为JavaBean ---> 提取事件事件生成watermark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "DWM_ORDER_WIDE";
        String groupId = "OrderWideApp";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 获取数据(两条流)
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.addSource(KafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .map(data -> {
                    // 转为JavaBean
                    // mysql中的字段名和JavaBean中的字段名不同，但是支持在驼峰和下划线格式之间识别
                    OrderInfo orderInfo = JSONObject.parseObject(data, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    String create_date = create_time.split(" ")[0];
                    String create_hour = create_time.split(" ")[1].split(":")[0];
                    long ts = simpleDateFormat.parse(create_time).getTime();
                    orderInfo.setCreate_date(create_date);
                    orderInfo.setCreate_hour(create_hour);
                    orderInfo.setCreate_ts(ts);
                    return orderInfo;
                })
                // 设置水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.addSource(KafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .map(data -> {
                    // 转为JavaBean
                    OrderDetail orderDetail = JSONObject.parseObject(data, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    long ts = simpleDateFormat.parse(create_time).getTime();
                    orderDetail.setCreate_ts(ts);
                    return orderDetail;
                })
                // 设置水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );


        // TODO: 双流join，实现关联操作
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDIMDS = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-5), Time.seconds(5))  // 生产环境中给的最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });


        // TODO: 关联维度信息（HBase中）
        // 使用异步传输关联维度数据
        // 1、用户维度数据
        SingleOutputStreamOperator<OrderWide> orderWideDSWithUserInfo = AsyncDataStream.unorderedWait(
                orderWideWithoutDIMDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO"){

                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //取出用户维度中的生日
                        String birthday = dimInfo.getString("BIRTHDAY");
                        long currentTS = System.currentTimeMillis();
                        Long ts = sdf.parse(birthday).getTime();

                        //将生日字段处理成年纪
                        Long ageLong = (currentTS - ts) / 1000L / 60 / 60 / 24 / 365;
                        input.setUser_age(ageLong.intValue());

                        //取出用户维度中的性别
                        String gender = dimInfo.getString("GENDER");
                        input.setUser_gender(gender);

                    }

                    @Override
                    public String getId(OrderWide input) {
                        return input.getUser_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 2、关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideDSWithLoc = AsyncDataStream.unorderedWait(
                orderWideDSWithUserInfo,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        //提取维度信息并设置进orderWide
                        input.setProvince_name(dimInfo.getString("NAME"));
                        input.setProvince_area_code(dimInfo.getString("AREA_CODE"));
                        input.setProvince_iso_code(dimInfo.getString("ISO_CODE"));
                        input.setProvince_3166_2_code(dimInfo.getString("ISO_3166_2"));
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return input.getProvince_id().toString();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 3、关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideDSWithSKU = AsyncDataStream.unorderedWait(
                orderWideDSWithLoc,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        //提取维度信息并设置进orderWide
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return String.valueOf(input.getSku_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 4、关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideDSWithSPU = AsyncDataStream.unorderedWait(
                orderWideDSWithSKU,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        //提取维度信息并设置进orderWide
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return String.valueOf(input.getSpu_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 5、关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideDSWithTradeMark = AsyncDataStream.unorderedWait(
                orderWideDSWithSPU,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        //提取维度信息并设置进orderWide
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return String.valueOf(input.getTm_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // 6、关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideDSWithCategory3 = AsyncDataStream.unorderedWait(
                orderWideDSWithTradeMark,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    protected void appendInfo(OrderWide input, JSONObject dimInfo) throws ParseException {
                        //提取维度信息并设置进orderWide
                        input.setCategory3_name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getId(OrderWide input) {
                        return String.valueOf(input.getCategory3_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        // TODO: 写入kafka
        orderWideDSWithCategory3
                .map(JSONObject::toJSONString)
                .addSink(KafkaUtil.getFlinkKafkaProducer(orderWideSinkTopic));

        // TODO: 执行
        env.execute("OrderWideApp");

    }
}
