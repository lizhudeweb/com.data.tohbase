package com.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将HDFS中的数据写入到HBase表中。
 * 没表要先创建
 * 先上传fruit.tsv
 * 
 */
public class HDFSToTableRunner extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		// 得到Configuration
		Configuration conf = this.getConf();

		// 创建Job任务
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		job.setJarByClass(HDFSToTableRunner.class);
//		Path inPath = new Path("hdfs://linux001:8020/input_fruit/fruit.tsv");
		Path inPath = new Path("hdfs://localhost:9000/user/datamgr/input_fruit/fruit.tsv");
		FileInputFormat.addInputPath(job, inPath);

		// 设置Mapper
		job.setMapperClass(HDFSToTableMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 设置Reducer
		TableMapReduceUtil.initTableReducerJob("fruit_hdfs", HDFSToTableReducer.class, job);

		// 设置Reduce数量，最少1个
		job.setNumReduceTasks(1);

		boolean isSuccess = job.waitForCompletion(true);
		if (!isSuccess) {
			throw new IOException("Job running with error");
		}

		return isSuccess ? 0 : 1;
	}

	// 4) 调用执行Job
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		int status = ToolRunner.run(conf, new HDFSToTableRunner(), args);
		System.exit(status);
	}
}
