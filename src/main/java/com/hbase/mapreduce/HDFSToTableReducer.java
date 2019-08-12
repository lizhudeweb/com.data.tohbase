package com.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class HDFSToTableReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
		System.out.println("============reduce start...");
		//读出来的每一行数据写入到fruit_hdfs表中
		for(Put put: values){
			context.write(NullWritable.get(), put);
		}
	}
}
