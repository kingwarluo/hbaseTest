package com.qiu.hbase;

import com.qiu.hbase.util.HbaseUtil;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Driver {

    public static void main(String[] args) throws InterruptedException {

        Thread thread1 = new Thread(new PutThread(1, 1000));
        thread1.run();
        Thread thread2 = new Thread(new PutThread(1000, 2000));
        thread2.run();

//        TimeUnit.SECONDS.sleep(20);
//        HbaseUtil.allCommit();

        ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);
        scheduled.scheduleAtFixedRate(new CommitThread(), 0, 1, TimeUnit.MINUTES);

        HbaseUtil.close();
        System.exit(0);
    }
}
