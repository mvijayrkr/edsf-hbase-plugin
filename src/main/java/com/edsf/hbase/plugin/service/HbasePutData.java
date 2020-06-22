package com.edsf.hbase.plugin.service;

import com.edsf.hbase.plugin.config.HBaseConfig;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HbasePutData {
    private static HBaseConfig conf;
    static void createTableAndWriteDataWithLabels(String... labelExps)
            throws Exception {
        HTable table = null;
        Admin admin;
        try {
           admin= conf.getHbaseAdmin(conf.getHbaseConnect());
            HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("test12"));
            htable.addFamily(new HColumnDescriptor("cf1").setCompressionType(Compression.Algorithm.NONE));
            htable.addFamily(new HColumnDescriptor("cf2").setCompressionType(Compression.Algorithm.NONE));
           // conf.getHbaseAdmin(conf.getHbaseConnect()).createTable();
            int i = 1;
            List<Put> puts = new ArrayList<Put>();
            for (String labelExp : labelExps) {
                Put put = new Put(Bytes.toBytes("row" + i));
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value1"));
                put.setCellVisibility(new CellVisibility(labelExp));

                puts.add(put);
                i++;
            }
            table = (HTable) conf.getHbaseConnect().getTable(TableName.valueOf("test12"));
            table.put(puts);
        }catch(Exception e)
        {
            e.printStackTrace();
    }
    }
    public static void main(String[] ary) throws Exception {
        HbasePutData hput= new HbasePutData();
        hput.createTableAndWriteDataWithLabels("private,secret");
    }
}
