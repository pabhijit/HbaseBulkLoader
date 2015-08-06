package com.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

	private static final String DATA_SEPARATOR = "\\|";	
	
	String tableName = null;
	String columnFamily = null;
	ImmutableBytesWritable hKey = new ImmutableBytesWritable();
	KeyValue kv;

	@Override
	protected void setup(Context context) throws IOException,
	InterruptedException {
		Configuration conf = context.getConfiguration();

		tableName = conf.get("hbase.table.name");
		columnFamily = conf.get("hbase.column.family");
	}

	/** {@inheritDoc} */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String[] fields = null;
		String delimitor = "\\|";
		int lengthf = 0;

		fields = value.toString().split( delimitor );
		lengthf = fields.length;

		hKey.set(String.format("%s", fields[0]).getBytes());

		// If field exists
		/*if ( lengthf!=0 && !fields[1].equals("") ) {

			// Save KeyValue Pair
			kv = new KeyValue(hKey.get(), columnFamily.getBytes(),"vendor_id".getBytes(), fields[1].getBytes());
			lengthf --;
			context.write(hKey, kv);
		}*/

		if ( lengthf != 0 && !fields[1].equals("") ) {
			kv = new KeyValue(hKey.get(), columnFamily.getBytes(),"vendor_name".getBytes(), fields[2].getBytes());
			lengthf --;
			context.write(hKey, kv);
		}
		
		if ( lengthf != 0 && !fields[2].equals("") ) {
			kv = new KeyValue(hKey.get(), columnFamily.getBytes(),"byr_cd".getBytes(), fields[2].getBytes());
			lengthf --;
			context.write(hKey, kv);
		}
		
		if ( lengthf != 0 && !fields[3].equals("") ) {
			kv = new KeyValue(hKey.get(), columnFamily.getBytes(),"byr_nam".getBytes(), fields[2].getBytes());
			lengthf --;
			context.write(hKey, kv);
		}

	}
}
