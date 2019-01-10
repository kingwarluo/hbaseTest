package com.qiu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.hbase.util.Bytes.toInt;

public class HbaseTest {

    public Configuration conf;
    public Admin admin;
    public Connection con;
    TableName tableName = TableName.valueOf("staff");
    byte[] family = Bytes.toBytes("cf1");
    byte[] family2 = Bytes.toBytes("cf2");

    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "qiu1,qiu2,qiu3");
//        con = ConnectionFactory.createConnection(conf);
        ExecutorService executor = Executors.newFixedThreadPool(10);
        con = ConnectionFactory.createConnection(conf, executor);
        admin = con.getAdmin();
    }

    @Test
    public void createTable() {

        try {
            ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(this.family).setMaxVersions(1).build();
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
    public void checkTable() throws IOException {
        boolean exists = admin.tableExists(tableName);
        System.out.println(exists);
    }

    @Test
    public void addColumnFamily() throws IOException {
        admin.disableTable(tableName);

//        HColumnDescriptor cf2 = new HColumnDescriptor("cf2");
//        admin.addColumn(tableName, cf2);      // adding new ColumnFamily

//        ColumnFamilyDescriptor cf2 = ColumnFamilyDescriptorBuilder.of("cf2");
        ColumnFamilyDescriptor cf2 = ColumnFamilyDescriptorBuilder.newBuilder(family2).setMaxVersions(2).build();
        admin.addColumnFamily(tableName, cf2);
        admin.enableTable(tableName);
    }

    @Test
    public void deleteColumnFamily() throws IOException {
        admin.disableTable(tableName);
        admin.deleteColumnFamily(tableName, family2);
        admin.enableTable(tableName);
    }

    @Test
    public void deleteTable() throws IOException {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    @Test
    public void insert() throws IOException {
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes(3));
//        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("depp"));
        put.addColumn(family2, Bytes.toBytes("weight"), Bytes.toBytes("140"));
        puts.add(put);
        Table table = null;
        try {
            table = con.getTable(tableName);
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                table.close();
        }
    }

    @Test
    public void addCell() throws IOException {
        List<Put> puts = new ArrayList<>();
        Put put = new Put(Bytes.toBytes(12348));
        put.addColumn(family, Bytes.toBytes("sex"), Bytes.toBytes("male"));
        puts.add(put);
        Table table = null;
        try {
            table = con.getTable(tableName);
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                table.close();
        }
    }

    @Test
    public void query() throws IOException {
        Table table = null;
        try {
            table = con.getTable(tableName);
            Get get = new Get(Bytes.toBytes(3));
            get.setMaxVersions(1);
            get.addColumn(family2, Bytes.toBytes("weight"));
//        get.addColumn(family, Bytes.toBytes("sex"));
            Result result = table.get(get);
            byte[] value = result.getValue(family2, Bytes.toBytes("weight"));
            byte[] sex = result.getValue(family2, Bytes.toBytes("sex"));
            System.out.println(Bytes.toString(value) + '\t' + Bytes.toString(sex));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                table.close();
        }
    }

    @Test
    public void scan() throws IOException {
        Table table = null;
        try {
            table = con.getTable(tableName);
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            for (Iterator<Result> it = scanner.iterator(); it.hasNext(); ) {
                Result result = it.next();
                String rowkey = String.valueOf(toInt(result.getRow()));
                System.out.print(rowkey + '\t' + Bytes.toString(result.getValue(family, Bytes.toBytes("name"))) + '\t');
                System.out.println(Bytes.toString(result.getValue(family2, Bytes.toBytes("weight"))));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                table.close();
        }
    }

    @Test
    public void output() throws Throwable {
        Path output = new Path("/tmp2/HbaseTest");
        Scan scan = new Scan();
        Map<byte[], Export.Response> result = Export.run(conf, tableName, scan, output);
        final long totalOutputRows = result.values().stream().mapToLong(v -> v.getRowCount()).sum();
        final long totalOutputCells = result.values().stream().mapToLong(v -> v.getCellCount()).sum();
        System.out.println("table:" + tableName);
        System.out.println("output:" + output);
        System.out.println("total rows:" + totalOutputRows);
        System.out.println("total cells:" + totalOutputCells);
    }

    @Test
    public void testTablePoll() {
    }

    @After
    public void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (con != null) {
            con.close();
        }
    }

}