package com.edsf.hbase.plugin.service;

import com.edsf.hbase.plugin.config.HBaseConfig;
import com.google.common.collect.ArrayListMultimap;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.security.SecurityCapability;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;

import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class HbaseGrantServiceImp {
   // private static final Logger LOG = LoggerFactory.getLogger(HbaseGrantServiceImp.class);
  // Logger log = LogManager.getLogger("Logger that references elasticsearchAsyncBatch");
    public static final TableName ACL_TABLE_NAME =
            TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "acl");

    @Autowired
    HBaseConfig conn = new HBaseConfig();

    //Return true if authorization is supported and enabled. false otherwise
    public static boolean isAuthorizationEnabled(Connection connection) throws IOException {
        return connection.getAdmin().getSecurityCapabilities()
                .contains(SecurityCapability.AUTHORIZATION);
    }

    //Return true if cell authorization is supported and enabled, false otherwise
    public boolean isCellAuthorizationEnabled(Connection connection) throws IOException {
        return connection.getAdmin().getSecurityCapabilities()
                .contains(SecurityCapability.CELL_AUTHORIZATION);
    }


    //Grant global permissions for the specified user.1
    public void grantGlobal(final String user, final Permission.Action... actions) throws Exception {
       // HBaseConfig conn = new HBaseConfig();
        try {
            AccessControlClient.grant(conn.getHbaseConnect(),user,actions);
        //    log.info("Granted Global Permissions for the user -> "+ user);

        } catch (Throwable t) {
           // throwable.printStackTrace();
          //  LOG.error("grantGlobal failed : ", t);
        }

    }

    // Grants permission on the specified namespace for the specified user.
    public void grantOnNamespace(final String user, final String namespace,
                                        final Permission.Action... actions) {

        try {
            AccessControlClient.grant(conn.getHbaseConnect(),namespace,user,actions);
        } catch (Throwable t) {
           // throwable.printStackTrace();
          //  LOG.error("grantOnNamespace failed : ", t);
        }

    }
   /* *//**
     * The AccessControlLists.addUserPermission may throw exception before closing the table.
     *//*
    private void addUserPermission(Configuration conf, UserPermission userPerm, Table t) throws IOException {
        try {
            AccessControlLists.addUserPermission(conf, userPerm, t);
        } finally {
            t.close();
        }
    }*/
    //Grants permission on the specified table for the specified user
    public void grantOnTable(final String user,
                                    final TableName table, final byte[] family, final byte[] qualifier,
                                    final Permission.Action... actions) throws Exception {
       /* try {
            AccessControlClient.grant(conn.getHbaseConnect(), table, user, family, qualifier, actions);
            Admin admin = conn.getHbaseConnect().getAdmin();
            System.out.println(admin.getDescriptor(table));*/

            try (Connection connection = conn.getHbaseConnect()) {
                connection.getAdmin().grant(new UserPermission(user, Permission.newBuilder(table)
                                .withFamily(family).withQualifier(qualifier).withActions(actions).build()),
                        false);
            }
        catch (Throwable t) {
           // throwable.printStackTrace();
          //  LOG.error("grantOnTable failed : ", t);
        }

            /*    try (Connection connection = ConnectionFactory.createConnection(util.getConfiguration())) {
                    connection.getAdmin().grant(new UserPermission(user, Permission.newBuilder(table)
                                    .withFamily(family).withQualifier(qualifier).withActions(actions).build()),
                            false);
                }
          */

    }

}
