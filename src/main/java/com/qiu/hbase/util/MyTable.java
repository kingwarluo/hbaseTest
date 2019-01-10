package com.qiu.hbase.util;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MyTable {
    Table table = null;
    private static List<Put> puts = new ArrayList<>();

    private static volatile long heapSize = 0;
    private static final int MAX_SIZE = 200000;

    private static long rowCount = 0;

    public MyTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void put(String tableName, Put put){
        put(tableName, put, false);
    }

    public void put(String tableName, Put put, boolean force) {
        heapSize += put.heapSize();
        rowCount++;
        puts.add(put);
        if (heapSize >= MAX_SIZE || force) {
            synchronized (tableName) {
                try {
                    System.out.println("heapSize commit: " + rowCount);
                    table.put(puts);
                    puts.clear();
                    heapSize = 0;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void commit(){
        try {
            if(puts.size() > 0) {
                System.out.println("all commit: " + rowCount);
                table.put(puts);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
