package com.qiu.hbase;

import com.qiu.hbase.util.HbaseUtil;

public class CommitThread implements Runnable {

    @Override
    public void run() {
        HbaseUtil.allCommit();
    }

}
