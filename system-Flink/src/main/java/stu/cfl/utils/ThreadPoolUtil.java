package stu.cfl.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    /**
     * 线程池工具包
     */
    private static ThreadPoolExecutor threadPoolExecutor = null;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getThreadPool(){
        if (threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                // 加锁，防止同时进入生成多个资源，导致浪费
                if (threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(8,
                            16,
                            1L,
                            TimeUnit.MINUTES,
                            new LinkedBlockingQueue<>()
                    );
                }
            }

        }
        return threadPoolExecutor;
    }
}
