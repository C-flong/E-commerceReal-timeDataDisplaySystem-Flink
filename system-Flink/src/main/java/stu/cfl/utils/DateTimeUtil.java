package stu.cfl.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    /**
     * 时间工具类，相比于 SimpleDateFormat 来说是线程安全的
     */

    private final static DateTimeFormatter formatterr = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formatterr.format(localDateTime);
    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatterr);

        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
