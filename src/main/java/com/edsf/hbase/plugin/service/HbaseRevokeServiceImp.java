package com.edsf.hbase.plugin.service;


import com.edsf.hbase.plugin.config.HBaseConfig;
import com.edsf.hbase.plugin.domain.RevokeAccessUsersResponse;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.TablePermission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HbaseRevokeServiceImp  {
    private static final Logger LOG = LoggerFactory.getLogger(HbaseGrantServiceImp.class);


    @Autowired
    HBaseConfig connection = new HBaseConfig();


    //Revokes the permission on the table
    public  void revokeFromTable(final String user, final TableName table, final byte[] family, final byte[] qualifier,
                                       final Permission.Action... actions) throws Exception {
        try {
        AccessControlClient.revoke(connection.getHbaseConnect(), table, user, family, qualifier, actions);

        } catch (Throwable t) {
            LOG.error("revokeFromTable failed: ", t);
        }
    }

    //Revokes the permission on the namespace for the specified user.
    public  void revokeFromNamespace( final String user, final String namespace,
                                                                   final Permission.Action... actions) throws Exception {

                try {
                    AccessControlClient.revoke(connection.getHbaseConnect(), namespace, user, actions);
                } catch (Throwable t) {
                    LOG.error("revoke failed: ", t);
                }
    }
    //Revoke global permissions for the specified user.
    public  void revokeGlobal(final String user,final Permission.Action... actions)
            throws Exception {

                try {
                    AccessControlClient.revoke(connection.getHbaseConnect(), user, actions);
                } catch (Throwable t) {
                    LOG.error("revoke failed: ", t);
                }

    }



}
