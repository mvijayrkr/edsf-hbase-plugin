package com.edsf.hbase.plugin.domain;

import com.edsf.hbase.plugin.config.HBaseConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;

public class AccessUsersBody {


    String namespace;
    User caller;
    HBaseConfig conn;
    String user;
    String table;
    String family;
    String qualifier;
    Permission.Action[] actions;

    public String getTable() {
        return table;
    }
    public String getFamily() {
        return family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public Permission.Action[] getActions() {
        return actions;
    }



    public AccessUsersBody() {
    }

    public AccessUsersBody(User caller, HBaseConfig conn, String user, Permission.Action[] actions, String namespace) {
        this.caller = caller;
        this.conn = conn;
        this.user = user;
        this.actions = actions;
        this.namespace = namespace;
    }

    public User getCaller() {
        return caller;
    }

    public HBaseConfig getConn() {
        return conn;
    }

    public String getUser() {
        return user;
    }

/*    public TableName getTable() {
        return table;
    }*/

 /*   public byte[] getFamily() {
        return family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }*/



    public String getNamespace() {
        return namespace;
    }

}
