package com.edsf.hbase.plugin.controller;

import com.edsf.hbase.plugin.config.HBaseConfig;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;



@RestController
public class connectionTest {
    @Autowired
    HBaseConfig hbaseConfig;
   // private final Logger logger = LoggerFactory.getLogger(this.getClass());
    //For checking the hbase connection- give the tableName as a parameter to Rest url.

    @RequestMapping("/scantable")
    public List<String> readRecordforConTest(@RequestParam(required = true) String tableName) throws IOException {

        Connection hbaseConnect = hbaseConfig.getHbaseConnect();
        List<String> scandata = new ArrayList<>();
        System.out.println("__________________________________________________");
        try (Table table = hbaseConnect.getTable(TableName.valueOf(tableName));
             ResultScanner rs = table.getScanner(new Scan()) ) {
            for(Result ret = rs.next(); ret != null; ret = rs.next()) {
                for (Cell cell : ret.listCells()) {
                    String rkey = Bytes.toString(ret.getRow());
                    String cf = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qual = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    scandata.add("Row key: " + rkey +
                            ", Column Family: " + cf +
                            ", Qualifier: " + qual +
                            ", Value : " + value);
                 /*   System.out.println("Row key: " + rkey +
                            ", Column Family: " + cf +
                            ", Qualifier: " + qual +
                            ", Value : " + value);*/
                }
            }
        }
        return scandata;
    }

    @RequestMapping("/listusertables")
    public String[]  listTablesForUser(@RequestParam(required = true) String username) throws IOException {
        User user = User.createUserForTesting(HBaseConfiguration.create(), username, new String[0]);
        Connection hbaseConnect = hbaseConfig.getHbaseConnect(user);

        Admin admin = hbaseConnect.getAdmin();
        TableName[] tables = admin.listTableNames();
        String[] tableslist = new String[tables.length];
        for(int i = 0; i< tables.length; i++) {
            tableslist[i]= tables[i].getNameAsString();
            System.out.println(tables[i].getNameAsString());
        }
        return new String[]{Arrays.toString(tableslist)};
    }
  /*  @Scheduled(fixedDelay = 1000L)
    void logSomeStuff(){

        logger.info("Log message generated");

    }*/
}
