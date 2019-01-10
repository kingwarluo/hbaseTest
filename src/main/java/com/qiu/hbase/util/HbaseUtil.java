package com.qiu.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HbaseUtil {
    private static Configuration conf;
    private static Admin admin;
    private static Connection con;
    private static final ConcurrentHashMap<String, MyTable> tableMap = new ConcurrentHashMap<String, MyTable>();

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata.kingwarluo.com");
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            con = ConnectionFactory.createConnection(conf, executor);
            admin = con.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Admin getAdmin() {
        return admin;
    }

    public static MyTable getTable(String tableName) {
        MyTable table = null;
        table = tableMap.get(tableName);
        if (table != null) {
            return table;
        }
        try {
            MyTable myTable = new MyTable(con.getTable(TableName.valueOf(tableName)));
            tableMap.put(tableName, myTable);
            table = myTable;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    public static void close() {
        try {
            for (Map.Entry<String, MyTable> entry : tableMap.entrySet()) {
                entry.getValue().getTable().close();
            }

            if (admin != null) {
                admin.close();
            }

            if (con != null) {
                con.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void allCommit() {
        try {
            for (Map.Entry<String, MyTable> entry : tableMap.entrySet()) {
                entry.getValue().commit();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
