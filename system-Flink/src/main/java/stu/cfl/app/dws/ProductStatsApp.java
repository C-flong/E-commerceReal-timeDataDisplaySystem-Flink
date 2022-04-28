package stu.cfl.app.dws;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import stu.cfl.app.function.DimAsyncFunction;
import stu.cfl.bean.OrderWide;
import stu.cfl.bean.PaymentWide;
import stu.cfl.bean.ProductStats;
import stu.cfl.common.Constant;
import stu.cfl.utils.ClickHouseUtil;
import stu.cfl.utils.DateTimeUtil;
import stu.cfl.utils.KafkaUtil;

import java.text.ParseException;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
/**
 * 商品主题宽表
 * 点击、曝光、收藏、加入购物车、下单、支付、退款、评价
 */
public class ProductStatsApp {

    public static void main(String[] args) throws Exception {

        // TODO 环境
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

        // TODO 从kafka获取数据
        String groupId = "ProductStatsApp";
        String pageViewSourceTopic = "DWD_PAGE_LOG";
        String orderWideSourceTopic = "DWM_ORDER_WIDE";
        String paymentWideSourceTopic = "DWM_PAYMENT_WIDE";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        FlinkKafkaConsumer<String> pageViewSource = KafkaUtil.getKafkaConsumer(pageViewSourceTopic,groupId);
        FlinkKafkaConsumer<String> orderWideSource = KafkaUtil.getKafkaConsumer(orderWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> paymentWideSource = KafkaUtil.getKafkaConsumer(paymentWideSourceTopic,groupId);
        FlinkKafkaConsumer<String> favorInfoSourceSource = KafkaUtil.getKafkaConsumer(favorInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> cartInfoSource = KafkaUtil.getKafkaConsumer(cartInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> refundInfoSource = KafkaUtil.getKafkaConsumer(refundInfoSourceTopic,groupId);
        FlinkKafkaConsumer<String> commentInfoSource = KafkaUtil.getKafkaConsumer(commentInfoSourceTopic,groupId);

        DataStreamSource<String> pageViewDS = env.addSource(pageViewSource);
        DataStreamSource<String> favorInfoDS = env.addSource(favorInfoSourceSource);
        DataStreamSource<String> orderWideDS = env.addSource(orderWideSource);
        DataStreamSource<String> paymentWideDS = env.addSource(paymentWideSource);
        DataStreamSource<String> cartInfoDS = env.addSource(cartInfoSource);
        DataStreamSource<String> refundInfoDS = env.addSource(refundInfoSource);
        DataStreamSource<String> commentInfoDS = env.addSource(commentInfoSource);


        // TODO 统一流数据为bean对象类型
        // 页面数据和曝光数据，由于一条曝光数据中可能涵盖多条数据，因此用flatmap
        SingleOutputStreamOperator<ProductStats> pageFMDS = pageViewDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                // 获取商品页面点击数据（通过good_detail来判断）
                JSONObject page = jsonObject.getJSONObject("page");
                String page_id = page.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String item_type = page.getString("item_type");
                if ("good_detail".equals(page_id) && "sku_id".equals(item_type)) {
                    out.collect(
                            ProductStats.builder()
                                    .sku_id(page.getLong("item"))
                                    .click_ct(1L)
                                    .ts(ts)
                                    .build()
                    );
                }

                // 曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject display = displays.getJSONObject(i);
                        if ("sku_id".equals(display.getString("item_type"))) {
                            // item_type中标识为sku_id的才是商品
                            out.collect(
                                    ProductStats.builder()
                                            .sku_id(display.getLong("item"))
                                            .display_ct(1L)
                                            .ts(ts)
                                            .build()
                            );
                        }
                    }
                }
            }
        });
        // 收藏数据
        SingleOutputStreamOperator<ProductStats> favorMDS = favorInfoDS.map(data -> {
            JSONObject jsonObject = JSONObject.parseObject(data);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        // 购物车数据
        SingleOutputStreamOperator<ProductStats> cartMDS = cartInfoDS.map(data -> {
            JSONObject jsonObject = JSONObject.parseObject(data);
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        // 订单数据
        SingleOutputStreamOperator<ProductStats> orderWideMDS = orderWideDS.map(data -> {
            OrderWide orderWide = JSONObject.parseObject(data, OrderWide.class);
            HashSet<Long> orderIds = new HashSet<Long>();
            orderIds.add(orderWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())  // 数量
//                    .order_amount(orderWide.getOrder_price())  // sku_id订单价格
                    .order_amount(orderWide.getTotal_amount())  // sku_id订单价格
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });
        // 支付数据
        SingleOutputStreamOperator<ProductStats> paymentWideMDS = paymentWideDS.map(data -> {
            PaymentWide paymentWide = JSONObject.parseObject(data, PaymentWide.class);
            HashSet<Long> orderIds = new HashSet<Long>();
            orderIds.add(paymentWide.getOrder_id());
            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
//                    .payment_amount(paymentWide.getOrder_price())
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });
        // 退款数据
        SingleOutputStreamOperator<ProductStats> refundInfoMDS = refundInfoDS.map(data -> {
            JSONObject jsonObject = JSONObject.parseObject(data);
            HashSet<Long> orderIds = new HashSet<Long>();
            orderIds.add(jsonObject.getLong("order_id"));
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });
        // 评论数据
        SingleOutputStreamOperator<ProductStats> commentInfoMDS = commentInfoDS.map(data -> {
            JSONObject jsonObject = JSONObject.parseObject(data);
            String appraise = jsonObject.getString("appraise");
            long goodCt = 0L;
            if (Constant.APPRAISE_GOOD.equals(appraise)) {
                // 好评
                goodCt = 1L;
            }
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });


        // TODO union
        DataStream<ProductStats> unionDS = pageFMDS.union(
                favorMDS,
                cartMDS,
                orderWideMDS,
                paymentWideMDS,
                refundInfoMDS,
                commentInfoMDS
        );

        // TODO 提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> DSWithWM = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                            @Override
                            public long extractTimestamp(ProductStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );

        // TODO 分组，开窗，聚合 按照sku_id分组，10秒的滚动窗口，结合增量聚合（累加值）和全量聚合（提取窗口信息）
        SingleOutputStreamOperator<ProductStats> filterDS = DSWithWM.filter(data -> data.getSku_id() != null);
        SingleOutputStreamOperator<ProductStats> reduceDS = filterDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {

                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());

                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                stats1.setOrder_ct((long) stats1.getOrderIdSet().size());
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                stats1.setRefund_order_ct((long) stats1.getRefundOrderIdSet().size());

                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                stats1.setPaid_order_ct((long) stats1.getPaidOrderIdSet().size());

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                                return stats1;

                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            // <IN, OUT, KEY, W extends Window>
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                // 取出数据
                                ProductStats data = input.iterator().next();
                                // 设置窗口开始和结束时间
                                data.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                                data.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                                out.collect(data);
                            }
                        }
                );

//        reduceDS.print();

        // TODO 关联维度信息
        // SKU
        SingleOutputStreamOperator<ProductStats> reduceDSWithSKU = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    protected void appendInfo(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSku_name(dimInfo.getString("SKU_NAME"));
                        input.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        input.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        input.setSpu_id(dimInfo.getLong("SPU_ID"));
                        input.setTm_id(dimInfo.getLong("TM_ID"));
                    }

                    @Override
                    public String getId(ProductStats input) {
                        return String.valueOf(input.getSku_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // SPU
        SingleOutputStreamOperator<ProductStats> reduceDSWithSPU = AsyncDataStream.unorderedWait(
                reduceDSWithSKU,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    protected void appendInfo(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }

                    @Override
                    public String getId(ProductStats input) {
                        return String.valueOf(input.getSpu_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // Category
        SingleOutputStreamOperator<ProductStats> reduceDSWithCategory3 = AsyncDataStream.unorderedWait(
                reduceDSWithSPU,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    protected void appendInfo(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setCategory3_name(dimInfo.getString("NAME"));
                    }

                    @Override
                    public String getId(ProductStats input) {
                        return String.valueOf(input.getCategory3_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        // TM
        SingleOutputStreamOperator<ProductStats> reduceDSWithTM = AsyncDataStream.unorderedWait(
                reduceDSWithCategory3,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    protected void appendInfo(ProductStats input, JSONObject dimInfo) throws ParseException {
                        input.setTm_name(dimInfo.getString("TM_NAME"));
                    }

                    @Override
                    public String getId(ProductStats input) {
                        return String.valueOf(input.getTm_id());
                    }
                },
                60,
                TimeUnit.SECONDS
        );

//        reduceDSWithTM.print();
        // TODO 写入ClickHouse
        String sql = "insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        reduceDSWithTM.addSink(ClickHouseUtil.<ProductStats>getSink(sql));

        // TODO 执行
        env.execute("ProductStatsApp");

    }
}
