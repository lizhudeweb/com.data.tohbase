package com.hbase.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;






/**
 * @Description: 旧版Api
 * window下要配置hosts
 * 
 * @time: 2018年12月28日 上午11:30:45
 */
public class HbaseOldApi {

	
	
	public static void main(String[] args) throws Exception {
		String tableName = "student2";
		String rowKey = "001";
		String rowKey2 = "002";
//		createTable(tableName, "id", "name", "name2");
//		addRowData(tableName, rowKey, "name", "nameFamily", "nameFamily1");
//		addRowData(tableName, rowKey, "name", "nameFamily2", "nameFamily2");
//		addRowData(tableName, rowKey, "name2", "nameFamily2", "nameFamily3");
//		dropTable(tableName);

//		getRowQualifier(tableName, rowKey, "name2", "nameFamily2");
//		deleteMultiRow(tableName, "001","002");
		
		String tableName2 = "fruit";
//		createTable(tableName2, "id", "info");
//		addRowData(tableName2, rowKey, "info", "name", "apple");
//		addRowData(tableName2, rowKey, "info", "color", "red");
//		addRowData(tableName2, rowKey2, "info", "name", "banana");
//		addRowData(tableName2, rowKey2, "info", "color", "blue");
		
		System.out.println(isTableExist("fruit_hdfs"));
//		getAllRows("fruit_hdfs");
//		dropTable("fruit_hdfs");
//		createTable("fruit_hdfs", "id", "info");
		
//		System.out.println(isTableExist("hbase_book"));
//		getAllRows("hbase_book");
//		dropTable("hbase_book");
//		getRow(tableName, rowKey);
	}
	
	
	//获取Configuration对象：
	public static Configuration conf;
	
	static{
		//使用HBaseConfiguration的单例方法实例化
		conf = HBaseConfiguration.create();
		//10.1.31.43 PC201807121520
//		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "10.1.31.76");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	//判断表是否存在：
	public static boolean isTableExist(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		//在HBase中管理、访问表需要先创建HBaseAdmin对象
//		Connection connection = ConnectionFactory.createConnection(conf);
//		HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
		HBaseAdmin admin = new HBaseAdmin(conf);
		return admin.tableExists(tableName);
	}
	
	
	//创建表
	public static void createTable(String tableName, String... columnFamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		//判断表是否存在
		if(isTableExist(tableName)){
			System.out.println(tableName + "已存在");
			//System.exit(0);
		}else{
			//创建表属性对象,表名需要转字节
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//创建多个列族
			for(String cf : columnFamily){
				descriptor.addFamily(new HColumnDescriptor(cf));
			}
			//根据对表的配置，创建表
			admin.createTable(descriptor);
			System.out.println("createTable：" + tableName + " success");
		}
	}

	//删除表
	public static void dropTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(isTableExist(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("dropTable：" + tableName + " success");
		}else{
			System.out.println(tableName + "不存在");
		}
	}
	
	
	//插入数据
	public static void addRowData(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException{
		//创建HTable对象
		HTable hTable = new HTable(conf, tableName);
		//向表中插入数据
		Put put = new Put(Bytes.toBytes(rowKey));
		//向Put对象中组装数据
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		hTable.put(put);
		hTable.close();
		System.out.println(tableName+" addRowData success");
	}
	//删除多行数据
	public static void deleteMultiRow(String tableName, String... rows) throws IOException{
		HTable hTable = new HTable(conf, tableName);
		List<Delete> deleteList = new ArrayList<Delete>();
		for(String row : rows){
			Delete delete = new Delete(Bytes.toBytes(row));
			deleteList.add(delete);
		}
		hTable.delete(deleteList);
		hTable.close();
		System.out.println(tableName+" deleteMultiRow success");
	}
	//得到所有数据
	public static void getAllRows(String tableName) throws IOException{
		HTable hTable = new HTable(conf, tableName);
		//得到用于扫描region的对象
		Scan scan = new Scan();
		//使用HTable得到resultcanner实现类的对象
		ResultScanner resultScanner = hTable.getScanner(scan);
		for(Result result : resultScanner){
			Cell[] cells = result.rawCells();
			for(Cell cell : cells){
				System.out.println("rowKey:" + Bytes.toString(CellUtil.cloneRow(cell)) +" colFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) 
				+ " colName:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
				System.out.println(" value:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
	}
	//得到某一行所有数据
	public static void getRow(String tableName, String rowKey) throws IOException{
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		//get.setMaxVersions();显示所有版本
	    //get.setTimeStamp();显示指定时间戳的版本
		Result result = table.get(get);
		for(Cell cell : result.rawCells()){
			System.out.println("rowKey:" + Bytes.toString(result.getRow()) + " colFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))+" colName:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
			System.out.println("timestamp:" + cell.getTimestamp());
		}
	}
	//获取某一行指定“列族:列”的数据
	public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException{
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result result = table.get(get);
		for(Cell cell : result.rawCells()){
			System.out.println("rowKey:" + Bytes.toString(result.getRow()) + " colFamily:" + Bytes.toString(CellUtil.cloneFamily(cell))+
					" colName:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
			System.out.println(" value:" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
	}

	
}
