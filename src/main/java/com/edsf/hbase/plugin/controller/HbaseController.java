package com.edsf.hbase.plugin.controller;

import com.edsf.hbase.plugin.config.HBaseConfig;
import com.edsf.hbase.plugin.domain.AccessGrantNamespace;
import com.edsf.hbase.plugin.domain.AccessUsersBody;
import com.edsf.hbase.plugin.domain.LabelAuthBody;
import com.edsf.hbase.plugin.service.HbaseGrantServiceImp;
import com.edsf.hbase.plugin.service.HbaseLabelAuthServiceImp;
import com.edsf.hbase.plugin.service.HbaseRevokeServiceImp;
import com.edsf.hbase.plugin.service.HbaseUserPermissionServiceImp;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;


import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
//@RequestMapping("/api")
public class HbaseController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    HBaseConfig hBaseConfig;
    /* // static final String TABLE_NAME = "box_device";
     Connection connection;

   public HbaseController() throws IOException {
          this.connection = hBaseConfig.getHbaseConnect();
     }
     public HbaseController(User caller) throws IOException {
          this.connection = hBaseConfig.getHbaseConnect(caller);
     }*/
    HbaseGrantServiceImp grantService = new HbaseGrantServiceImp();
    HbaseRevokeServiceImp revokeService = new HbaseRevokeServiceImp();
    HbaseLabelAuthServiceImp labelService = new HbaseLabelAuthServiceImp();
    HbaseUserPermissionServiceImp permService = new HbaseUserPermissionServiceImp();
    /*
     *
     * Grants Access controller
     *
     */

    @RequestMapping(value = "/edsf/hbase/v1/grantglobalaccess", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public String grantGlobalAccess(@RequestBody AccessUsersBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        try {
            System.out.println("-----------------------");
            System.out.println(Permission.Action.values());
            System.out.println("-----------------------");
            grantService.grantGlobal(body.getUser(), body.getActions());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Access Granted Successfully";
    }
    @RequestMapping(value = "/index")
    public String index() {
        return "index";
    }



  @RequestMapping(value = "/edsf/hbase/v1/grantnamespaceaccess", method = RequestMethod.POST , consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public String grantNamespaceAccess(@RequestBody AccessUsersBody body) {
        try {

            grantService.grantOnNamespace(body.getUser(), body.getNamespace(), body.getActions());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Access Granted Successfully";
    }



    @RequestMapping(value = "/edsf/hbase/v1/granttableaccess", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void grantTableAccess(@RequestBody AccessUsersBody body) {
        try {
            //TODO: implement switch....

            //if the access request is for single column specific to one colfamily (ex: family: cf1, columnqualifeir: cq1)
            if(!body.getQualifier().contains(":") && !body.getFamily().contains(",")){
                grantService.grantOnTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(body.getFamily()), Bytes.toBytes(body.getQualifier()), body.getActions());
            }
            //This executes only if multiple columns are requested for the access, we need to have colfam with colqual as(cf1:cq1) to grant access to the requested column.
            //cf1:cq1,cf1:cq2,cf2:cq1,cf2:cq5
            if (!body.getQualifier().equals("") && body.getQualifier().contains(":")) {
                List<String> cqList = Arrays.asList(body.getQualifier().split(","));
                System.out.println(body.getQualifier().split(",").length);
                for (String cqsplit : cqList) {//cf1:cq1
                    System.out.println(cqsplit);
                    String[] cf_cq_split = cqsplit.split(":");
                    grantService.grantOnTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(cf_cq_split[0]), Bytes.toBytes(cf_cq_split[1]), body.getActions());
                }
            }

            //This executes if only columnfamilies are requested for access(single/multiple) for all the columns under requested columnfamilies
            if (body.getQualifier().equals("")) {

            //cf1,cf2,cf3
            List<String> cfList= Arrays.asList(body.getFamily().split(","));
            System.out.println(cfList.toString());
            for(String cf : cfList){
                System.out.println(cf);
                grantService.grantOnTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(cf), Bytes.toBytes(body.getQualifier()), body.getActions());
            }

            }



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /*
     *
     *  Revoke Access controller
     *
     */

    @RequestMapping(value = "/edsf/hbase/v1/revokeglobalaccess", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void revokeGlobalAccess(@RequestBody AccessUsersBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        try {
            revokeService.revokeGlobal(body.getUser(), body.getActions());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @RequestMapping(value = "/edsf/hbase/v1/revoketableaccess", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void revokeTableAccess(@RequestBody AccessUsersBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);

        try {
        /*    revokeService.revokeFromTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(body.getFamily()), Bytes.toBytes(body.getQualifier()), body.getActions());
        } catch (Exception e) {
            e.printStackTrace();
        }*/

        if(!body.getQualifier().contains(":") && !body.getFamily().contains(",")){
            revokeService.revokeFromTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(body.getFamily()), Bytes.toBytes(body.getQualifier()), body.getActions());
        }
        //This executes only if multiple columns are requested for the access, we need to have colfam with colqual as(cf1:cq1) to grant access to the requested column.
        //cf1:cq1,cf1:cq2,cf2:cq1,cf2:cq5
        if (!body.getQualifier().equals("") && body.getQualifier().contains(":")) {
            List<String> cqList = Arrays.asList(body.getQualifier().split(","));
            System.out.println(body.getQualifier().split(",").length);
            for (String cqsplit : cqList) {//cf1:cq1
                System.out.println(cqsplit);
                String[] cf_cq_split = cqsplit.split(":");
                revokeService.revokeFromTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(cf_cq_split[0]), Bytes.toBytes(cf_cq_split[1]), body.getActions());
            }
        }
        //This executes if only columnfamilies are requested for access(single/multiple) for all the columns under requested columnfamilies
        if (body.getQualifier().equals("")) {

            //cf1,cf2,cf3
            List<String> cfList= Arrays.asList(body.getFamily().split(","));
            System.out.println(cfList.toString());
            for(String cf : cfList){
                System.out.println(cf);
                revokeService.revokeFromTable(body.getUser(), TableName.valueOf(body.getNamespace(), body.getTable()), Bytes.toBytes(cf), Bytes.toBytes(body.getQualifier()), body.getActions());
            }

        }
    } catch (Exception e) {
        e.printStackTrace();
    }


}

    @RequestMapping(value = "/edsf/hbase/v1/revokenamespaceaccess", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public String revokeNamespaceAccess(@RequestBody AccessUsersBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        try {
            revokeService.revokeFromNamespace(body.getUser(), body.getNamespace(), body.getActions());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Revoke Successful";
    }

    /*
     *
     * Visibility Label Access controller
     *
     */
    @RequestMapping(value = "/edsf/hbase/v1/addlabels", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> addLabels(@RequestBody LabelAuthBody body) {
        List<String> addedLabels = new ArrayList<>();
        try {
            String[] labels = body.getLabel().split(",");
            addedLabels = labelService.addLabels(labels);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return addedLabels;
    }

    @RequestMapping(value = "/edsf/hbase/v1/listlabels", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> listLabels(@RequestParam String labelregex) {
        List<String> labellist = new ArrayList<>();
        try {
            labellist = labelService.listLables(labelregex);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return labellist;
    }

    @RequestMapping(value = "/edsf/hbase/v1/setuserlabelauth", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> setUserLabelAuth(@RequestBody LabelAuthBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        List<String> UserLabelAuth = new ArrayList<>();
        try {
            String[] labels = body.getLabel().split(",");
            UserLabelAuth = labelService.setLablesToUser(labels,body.getUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return UserLabelAuth;
    }

    @RequestMapping(value = "/edsf/hbase/v1/getuserlabelauth", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getUserLabelAuth(@RequestBody LabelAuthBody body) {
        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        List<String> userLabels = new ArrayList<>();
        try {
            userLabels = labelService.getUserTaggedLabels(body.getUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return userLabels;
    }

    @RequestMapping(value = "/edsf/hbase/v1/removeuserlabelauth", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> removeUserLabelAuth(@RequestBody LabelAuthBody body) {

        // GrantAccessUsersBody bdy = new GrantAccessUsersBody(body.);
        List<String> userremovedLabels = new ArrayList<>();
            String[] labels=body.getLabel().split(",");
            try {
                userremovedLabels = labelService.removeUserTaggedLabels(labels,body.getUser());
        } catch (Exception e) {
            e.printStackTrace();
        }
            return userremovedLabels;
    }

    @RequestMapping(value = "/edsf/hbase/v1/getallTablePermissions", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getallTablePermissions(@RequestParam String tableregx) {
        List<UserPermission> ups = null;
        List<String> scandata = new ArrayList<>();
        try {
         //   ".*" -> all tables
            ups = AccessControlClient.getUserPermissions(hBaseConfig.getHbaseConnect(), tableregx); // co AccessControlExample-06-PrintPerms Print the current permissions for all tables.
            System.out.println("Superuser: User permissions:");
            for (UserPermission perm : ups) {
                scandata.add("  " + perm);
               // System.out.println("  " + perm);
            }
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
        return scandata;
    }

    @RequestMapping(value = "/edsf/hbase/v1/getuserTablePermissions", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getuserTablePermissions(@RequestParam String tableregx,@RequestParam String user) {
        List<UserPermission> ups = null;
        List<String> scandata = new ArrayList<>();
        try {
            ups = AccessControlClient.getUserPermissions(hBaseConfig.getHbaseConnect(),tableregx,user); // co AccessControlExample-06-PrintPerms Print the current permissions for all tables.
            System.out.println("Superuser: User permissions:");
            for (UserPermission perm : ups) {
                scandata.add("  " + perm);
                // System.out.println("  " + perm);
            }
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
        return scandata;
    }


        @RequestMapping(value = "/edsf/hbase/v1/getusercfallTablePermissions", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getusercfallTablePermissions(@RequestParam String tableregx,@RequestParam String col_fam,@RequestParam String user) {
        List<UserPermission> ups = null;
        List<String> scandata = new ArrayList<>();
        try {
            ups = AccessControlClient.getUserPermissions(hBaseConfig.getHbaseConnect(),tableregx,Bytes.toBytes(col_fam),user ); // co AccessControlExample-06-PrintPerms Print the current permissions for all tables.
            System.out.println("Superuser: User permissions:");
            for (UserPermission perm : ups) {
                scandata.add("  " + perm);
                // System.out.println("  " + perm);
            }
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
        return scandata;
    }


    @RequestMapping(value = "/edsf/hbase/v1/getuserallTablePermissions", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public List<String> getuserallTablePermissions(@RequestParam String tableregx,@RequestParam String col_fam, @RequestParam String cols,@RequestParam String user) {
        List<UserPermission> ups = null;
        List<String> scandata = new ArrayList<>();
        try {
            ups = AccessControlClient.getUserPermissions(hBaseConfig.getHbaseConnect(), tableregx,Bytes.toBytes(col_fam),Bytes.toBytes(cols),user); // co AccessControlExample-06-PrintPerms Print the current permissions for all tables.
            System.out.println("Superuser: User permissions:");
            for (UserPermission perm : ups) {
                scandata.add("  " + perm);
                // System.out.println("  " + perm);
            }
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
        return scandata;
    }
     // create table and insert data with labels
     @RequestMapping(value = "/edsf/hbase/v1/createTableAndWriteDataWithLabels", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public void createTableAndWriteDataWithLabels(@RequestParam String tablename,@RequestParam String... labelExps) throws Exception {
        try (Admin admin = hBaseConfig.getHbaseAdmin(hBaseConfig.getHbaseConnect())) {
            HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(tablename));
            htable.addFamily(new HColumnDescriptor("cf1").setCompressionType(Compression.Algorithm.NONE));
            htable.addFamily(new HColumnDescriptor("cf2").setCompressionType(Compression.Algorithm.NONE));
            hBaseConfig.getHbaseAdmin(hBaseConfig.getHbaseConnect()).createTable(htable);
            int i = 1;
            List<Put> puts = new ArrayList<Put>();
            for (String labelExp : labelExps) {
                Put put = new Put(Bytes.toBytes("row" + i));
                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), HConstants.LATEST_TIMESTAMP, Bytes.toBytes("value1"));
                put.setCellVisibility(new CellVisibility(labelExp));
                puts.add(put);
                i++;
            }
            Table table = hBaseConfig.getHbaseConnect().getTable(TableName.valueOf(tablename));
            table.put(puts);
        }catch(Exception e)
    {
        e.printStackTrace();
    }
}
    @RequestMapping(value = "/edsf/hbase/v1/printUserPermissions", method = RequestMethod.GET, consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
   public  List<UserPermission> printUserPermissions(@RequestParam String tableRegex) throws Exception {
      List<UserPermission> permissions= permService.printUserPermissions(tableRegex);
      return permissions;
   }
}
