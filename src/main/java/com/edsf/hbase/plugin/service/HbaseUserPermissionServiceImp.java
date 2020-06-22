package com.edsf.hbase.plugin.service;

import com.edsf.hbase.plugin.config.HBaseConfig;
import com.edsf.hbase.plugin.domain.GetUserPermissions;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;


@Service
public class HbaseUserPermissionServiceImp {

/* List all the userPermissions matching the given pattern. If pattern is null, the behavior is
 dependent on whether user has global admin privileges or not. If yes, the global permissions
  along with the list of superusers would be returned. Else, no rows get returned.
 */
    @Autowired
    HBaseConfig hconf = new HBaseConfig();

    public List<UserPermission> getPermissionForTable(String tableName) throws Throwable {
        return AccessControlService.getUserPermissions(hconf.getHbaseConnect(),tableName);
    }
     // List all the userPermissions matching the given table pattern and user name.
     public List<UserPermission> getTablePermissionForUser(HBaseConfig connection, String tableName,String user) throws Throwable {
         return AccessControlService.getUserPermissions(connection.getHbaseConnect(),tableName, user);
     }

    //  List all the userPermissions matching the given table pattern and column family.
    public  List<UserPermission> getTablePermissionWithColumnFam(HBaseConfig connection, String tableName,String columnFamily) throws Throwable {
        return AccessControlService.getUserPermissions(connection.getHbaseConnect(),tableName,Bytes.toBytes(columnFamily));
    }
    //  List all the userPermissions matching the given table pattern, column family and user name.
    public  List<UserPermission> getTablePermissionWithColumnFamForUser(HBaseConfig connection, String tableName,String columnFamily,String user) throws Throwable {
        return AccessControlService.getUserPermissions(connection.getHbaseConnect(),tableName,Bytes.toBytes(columnFamily),user);
    }
    //  List all the userPermissions matching the given table pattern, column family and column based on table
    public  List<UserPermission> getallresourcePermission(HBaseConfig connection, String tableName,String columnFamily, String columnQualifier) throws Throwable {
        return AccessControlService.getUserPermissions(connection.getHbaseConnect(),tableName,Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier));
    }
    //  List all the userPermissions matching the given table pattern, column family and column qualifier based on user
    public  List<UserPermission> getallresourcePermissionForUser(HBaseConfig connection, String tableName,String columnFamily,String user, String columnQualifier) throws Throwable {
        return AccessControlService.getUserPermissions(connection.getHbaseConnect(),tableName,Bytes.toBytes(columnFamily),Bytes.toBytes(columnQualifier),user);
    }

    //haspermission------
   //  Validates whether specified user has permission to perform actions on the mentioned table, column family or column qualifier.
    public static boolean hasPermission(HBaseConfig connection, String tableName, String columnFamily,
                                        String columnQualifier, String userName, Permission.Action... actions) throws Throwable {
        return AccessControlService.hasPermission(connection.getHbaseConnect(), tableName, Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnQualifier), userName, actions);
    }

    public List<UserPermission> printUserPermissions(final String tableRegex) throws Exception {
        List<UserPermission> ups;
        try {
            ups= AccessControlClient.
                            getUserPermissions(hconf.getHbaseConnect(), tableRegex);
                    System.out.println(": User permissions (" +
                            (tableRegex != null ? tableRegex : "hbase:acl") + "):");
                    int count = 0;
                    for (UserPermission perm : ups) {
                        System.out.println("  " + perm);
                        count++;
                    }
                    System.out.println("Found " + count + " permissions.");
                } catch (Throwable throwable) {
                    throw new RuntimeException(throwable);
                }
    return ups;
    }

}
