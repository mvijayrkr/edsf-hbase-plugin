package com.edsf.hbase.plugin.service;

import com.edsf.hbase.plugin.config.HBaseConfig;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.VisibilityLabelsProtos;
import org.apache.hadoop.hbase.security.User;

import org.apache.hadoop.hbase.security.visibility.VisibilityClient;
import org.apache.hadoop.hbase.security.visibility.VisibilityConstants;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.RegionActionResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.*;

@Service
public class HbaseLabelAuthServiceImp {

    @Autowired
    HBaseConfig conn = new HBaseConfig();


    public List<String> addLabels(String[] labels) throws IOException {
           // String[] labels = { "sensitive", "non-sensitive", "confidential", "pii"};

        VisibilityLabelsProtos.VisibilityLabelsResponse labelsadded;
            try {
                labelsadded =VisibilityClient.addLabels(conn.getHbaseConnect(), labels);
                } catch (Throwable t) {
                   throw new IOException(t);
                 }
        List<String> authsList = new ArrayList<>(labelsadded.getResultList().size());
        for (ClientProtos.RegionActionResult authBS :labelsadded.getResultList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        return authsList;
    }


    public List<String> listLables(String regexlist) throws IOException {
        /**
         * Retrieve the list of visibility labels defined in the system.
         * @param regex  The regular expression to filter which labels are returned.
         * @return List of visibility labels
         *   List<String> listLabels(String regex) throws IOException;
         */
        String label = "pii";
        VisibilityLabelsProtos.ListLabelsResponse labelsList;
        try {
            labelsList = VisibilityClient.listLabels(conn.getHbaseConnect(), regexlist);
            System.out.println(labelsList.getLabelList());
        } catch (Throwable t) {
            throw new IOException(t);
        }
        List<String> authsList = new ArrayList<>(labelsList.getLabelList().size());
        for (ByteString authBS : labelsList.getLabelList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        return authsList;
    }


    public List<String> setLablesToUser(String[] labels, String user) throws IOException, InterruptedException  {
        /**
         * Sets given labels globally authorized for the user.
         * @param user
         *          The authorizing user
         * @param authLabels
         *          Labels which are getting authorized for the user
         * @return OperationStatus for each of the label auth addition
         *  OperationStatus[] setAuths(byte[] user, List<byte[]> authLabels) throws IOException;
         */
       // String[] labels = { "sensitive", "non-sensitive", "confidential", "pii"};
        VisibilityLabelsProtos.VisibilityLabelsResponse userlabels;
        try{
            userlabels = VisibilityClient.setAuths(conn.getHbaseConnect(), labels, user);

          //  VisibilityLabelsProtos.VisibilityLabel.

           } catch (Throwable t) {
           throw new IOException(t);
       }
        List<String> authsList = new ArrayList<>(userlabels.getResultList().size());
        for (ClientProtos.RegionActionResult authBS :userlabels.getResultList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        return authsList;
    }


    public List<String> getUserTaggedLabels(String user) throws IOException {
        /**
         * Retrieve the visibility labels for the user.
         * @param user
         *          Name of the user whose authorization to be retrieved
         * @param systemCall
         *          Whether a system or user originated call.
         * @return Visibility labels authorized for the given user.
         *   List<String> getUserAuths(byte[] user, boolean systemCall) throws IOException;
         */
        VisibilityLabelsProtos.GetAuthsResponse labels;
        try {
             labels = VisibilityClient.getAuths(conn.getHbaseConnect(),user);
            System.out.println(labels.getAuthList());
        }catch (Throwable t) {
            throw new IOException(t);
        }
        List<String> authsList = new ArrayList<>(labels.getAuthList().size());
        for (ByteString authBS : labels.getAuthList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        return authsList;
    }


    public List<String> removeUserTaggedLabels(String[] labels, String user) throws IOException {
/**
 * Removes given labels from user's globally authorized list of labels.
 * @param user
 *          The user whose authorization to be removed
 * @param authLabels
 *          Labels which are getting removed from authorization set
 * @return OperationStatus for each of the label auth removal
 * OperationStatus[] clearAuths(byte[] user, List<byte[]> authLabels) throws IOException;
 */
       // String[] labelstest = { "sensitive", "non-sensitive", "confidential", "pii"};
        VisibilityLabelsProtos.VisibilityLabelsResponse removedlabels;
        try  {
            removedlabels = VisibilityClient.clearAuths(conn.getHbaseConnect(),labels,user);
        } catch (Throwable t) {
        throw new IOException(t);
    }
        List<String> authsList = new ArrayList<>(removedlabels.getResultList().size());
        for (ClientProtos.RegionActionResult authBS : removedlabels.getResultList()) {
            authsList.add(Bytes.toString(authBS.toByteArray()));
        }
        return authsList;
    }


    public void getgroupTaggedLabels() {
        /**
         * Retrieve the visibility labels for the groups.
         * @param groups
         *          Name of the groups whose authorization to be retrieved
         * @param systemCall
         *          Whether a system or user originated call.
         * @return Visibility labels authorized for the given group.
         * List<String> getGroupAuths(String[] groups, boolean systemCall) throws IOException;
         */


    }


    public void havingAuthorization() {
        /**
         * System checks for user auth during admin operations. (ie. Label add, set/clear auth). The
         * operation is allowed only for users having system auth. Also during read, if the requesting
         * user has system auth, he can view all the data irrespective of its labels.
         * @param user
         *          User for whom system auth check to be done.
         * @return true if the given user is having system/super auth
         *
         *  boolean havingSystemAuth(User user) throws IOException;
         */


    }
}
