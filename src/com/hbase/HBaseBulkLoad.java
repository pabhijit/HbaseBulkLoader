package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class HBaseBulkLoad {	
   
    public static void doBulkLoad(String hFilePath, HTable hTable) {
        try {		
            Configuration configuration = new Configuration();		
            configuration.set("hbase.zookeeper.quorum", "devhdp01");
            configuration.set("zookeeper.znode.parent","/hbase");
            configuration.set("hbase.master", "devhdp01:60000");
            configuration.set( "mapreduce.child.java.opts", "-Xmx10g" );	
            HBaseConfiguration.addHbaseResources( configuration );	
            LoadIncrementalHFiles loadFfiles = new LoadIncrementalHFiles( configuration );	
            loadFfiles.doBulkLoad( new Path( hFilePath ), hTable );	
            System.out.println( "Bulk Load Completed Succesfully" );		
        } catch( Exception exception ) {			
            exception.printStackTrace();			
        }		
    }	
}
