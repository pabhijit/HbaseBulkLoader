package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HBaseBulkLoadDriver {	

	/**
	 * HBase bulk import example
	 * Data preparation MapReduce job driver
	 * 
	 * args[0]: HDFS input path
	 * args[1]: HDFS output path
	 * args[2]: Table name
	 * args[3]: Column Family name
	 */
	public static void main(String[] args) {
		
		try{
			Configuration conf = new Configuration();

			// Pass parameters to Map Reduce
			conf.set("hbase.table.name", args[2]);
			conf.set("hbase.column.family", args[3]);

			// Load hbase-site.xml, core-site.xml.....
			HBaseConfiguration.addHbaseResources(conf);

			// Create the job
			Job job = new Job(conf, "HBase Bulk Import");
			job.setJarByClass(HBaseBulkLoadMapper.class);
			job.setMapperClass(HBaseBulkLoadMapper.class);
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(KeyValue.class);

			job.setInputFormatClass(TextInputFormat.class);

			// Get the table
			HTable hTable = new HTable(conf, args[2]);

			// Auto configure partitioner and reducer
			HFileOutputFormat.configureIncrementalLoad(job, hTable);

			// Save output path and input path
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// Wait for HFiles creations
			job.waitForCompletion(true);

			// Load generated HFiles into table
			LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
			HBaseBulkLoad.doBulkLoad(args[1], hTable);
		}catch(Exception e){
			e.printStackTrace();
		}

	}
}
