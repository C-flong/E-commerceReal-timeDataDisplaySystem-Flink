package stu.cfl.controller;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        /**
         * 获取伪数据，落盘，并传入kafka
         */

        // 落盘
        //        log.debug(jsonStr);
        log.info(jsonStr);
        //        log.warn(jsonStr);
        //        log.error(jsonStr);
        //        log.trace(jsonStr);

        // 写入kafka
        kafkaTemplate.send("ODS_BASE_LOG", jsonStr);

        return "success";
    }
}
