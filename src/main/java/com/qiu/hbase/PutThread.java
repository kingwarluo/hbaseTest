package com.qiu.hbase;

import com.qiu.hbase.util.HbaseUtil;
import com.qiu.hbase.util.MyTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PutThread implements Runnable {

    private static String tableName = "driver";
    private byte[] family = Bytes.toBytes("cf1");
    private byte[] column = Bytes.toBytes("content");

    private int start;
    private int end;

    public PutThread(int start, int end){
        this.start = start;
        this.end = end;
    }

    @Override
    public void run() {
        MyTable table = HbaseUtil.getTable(tableName);
        for (int i = start; i < end; i++) {
            Put put = new Put(Bytes.toBytes(i));
            String value = String.valueOf(i) + System.currentTimeMillis();
            put.addColumn(family, column, Bytes.toBytes(value));
            table.put(tableName, put);
        }
    }

}
