package com.qiu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.Export;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchTest {

    private Configuration conf;
    private Admin admin;
    private Connection con;
    Table table;
    private TableName tableName = TableName.valueOf("batch");
    private byte[] family = Bytes.toBytes("cf1");
    private byte[] column = Bytes.toBytes("content");

    @Before
    public void before() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata.kingwarluo.com");
        conf.set("hbase.table.sanity.checks", "false");
        ExecutorService executor = Executors.newFixedThreadPool(10);
        con = ConnectionFactory.createConnection(conf, executor);
        admin = con.getAdmin();
    }

    @Test
    public void createTable() {

        try {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(this.family)
                    .setMaxVersions(1).setInMemory(true).build();
            TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                    .setCoprocessor(Export.class.getName())
                    .setColumnFamily(familyDescriptor)
                    .build();
            admin.createTable(desc);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTable() throws IOException {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    @Test
    public void singlePut() {
        Long start = System.currentTimeMillis();
        try {
            table = con.getTable(tableName);
            for (int i = 1; i < 10000; i++) {
                Put put = new Put(Bytes.toBytes(i));
                String value = String.valueOf(i) + System.currentTimeMillis();
                put.addColumn(family, column, Bytes.toBytes(value));
                table.put(put);
            }
            System.out.println(System.currentTimeMillis() - start); //29931
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void batchPut() {
        Long start = System.currentTimeMillis();
        List<Put> puts = new ArrayList<>();
        try {
            table = con.getTable(tableName);
            for (int i = 10000; i < 20000; i++) {
                Put put = new Put(Bytes.toBytes(i));
                String value = String.valueOf(i) + System.currentTimeMillis();
                put.addColumn(family, column, Bytes.toBytes(value));
                puts.add(put);
            }
            table.put(puts);
            System.out.println(System.currentTimeMillis() - start); //2209
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() throws IOException {
        if (table != null) {
            table.close();
        }
        if (admin != null) {
            admin.close();
        }
        if (con != null) {
            con.close();
        }
    }

}
