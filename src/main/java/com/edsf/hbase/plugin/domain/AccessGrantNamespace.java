package com.edsf.hbase.plugin.domain;

import com.edsf.hbase.plugin.config.HBaseConfig;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.Permission;

public class AccessGrantNamespace {


    String namespace;
    String user;
    Permission.Action[] actions;

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Permission.Action[] getActions() {
        return actions;
    }

    public void setActions(Permission.Action[] actions) {
        this.actions = actions;
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



}
