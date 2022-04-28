package stu.cfl.service;

import com.alibaba.fastjson.JSONObject;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

@Service
public interface ScreenService {

    // TODO: 2022/4/10 查询总成交金额
    /**
     * 查询总成交金额
     * 组件：翻牌器
     * @return 返回成交总金额
     */
    public BigDecimal queryOrderAmount();


    // TODO: 2022/4/10 查询品牌排行
    /**
     * 查询品牌销售数量排行
     * 4 水平柱状图
     * @return
     */
    public List<JSONObject> queryTMName();


    // TODO: 2022/4/10 按照品类、spu分组后，各sku销售占比
    /**
     * 按照品类、spu分组后，各sku销售占比
     * 3 饼图
     * @return
     */
    public List<JSONObject> queryCategory();


    // TODO: 2022/4/10 查询商品spu的好评占比
    /**
     * 商品spu的好评占比
     * 6 轮播饼图
     * @return
     */
    public List<JSONObject> querySPU();

    // TODO: 2022/4/10 查询各省份购买情况
    /**
     *
     * @return
     */
    public List<JSONObject> queryProvince();

    // TODO: 2022/4/10 查询新老用户情况
    public List<JSONObject> queryVisitors();

    // TODO: 2022/4/10 查询分时流量
    public List<JSONObject> queryTraffic();

    // TODO: 2022/4/10 词云统计
    public List<JSONObject> queryKeyWords();


}
