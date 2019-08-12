package com.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

public class TableToCopyTableMapper extends TableMapper<ImmutableBytesWritable, Put> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		// 将fruit的name和color提取出来，相当于将每一行数据读取出来放入到Put对象中。
		Put put = new Put(key.get());
		System.out.println("============map start...");
		// 遍历添加column行
		for (Cell cell : value.rawCells()) {
			// 列族:info
			if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
				if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					//列：name
					//添加到输出
					put.add(cell);
				} else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					//列:color
					put.add(cell);
				}
			}
		}
		// 将从fruit读取到的每行数据写入到context中作为map的输出
		context.write(key, put);
	}
}
