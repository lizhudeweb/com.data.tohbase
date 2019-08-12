package com.hbase.business;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Bytes.toBytes()为hadoop方法,String.getBytes()为java方法。
 * toBytes()方法是将参数使用UTF-8的编码格式转换成byte[],getBytes()是用读取file.encoding的编码格式,然后用读取的格式进行转换
 * 所以,getBytes转换的byte[]的格式取决于操作系统和用户设置
 * 最好统一只用toBytes()方法。此外,去看下toBytes()的源码就可以发现,底层的实现是调用的getBytes()方法
 *
 * mg单身团业务  mgsr
 */
@SuppressWarnings("unused")
public class Business {
	
	//要创建的connection
	private static Connection connection = null;

	private static final String NS_MGSR = "ns_mgsr";
	private static final byte[] TABLE_CONTENT = Bytes.toBytes("ns_mgsr:content");
	private static final byte[] TABLE_RELATION = Bytes.toBytes("ns_mgsr:relation");
	private static final byte[] TABLE_INBOX = Bytes.toBytes("ns_mgsr:inbox");
	
	
	/**
	 * HBase客户端默认的是连接池大小是1，也就是每个RegionServer 1个连接。
	 */
	private Connection getInstance() throws IOException {
		if(null != connection) {
			return connection;
		}
		Configuration conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum", "127.0.0.1");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "linux000");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		connection =  ConnectionFactory.createConnection(conf);
		return connection;
	}
	
	//命名空间
	private void initNamespace(String nameSpace) throws IOException {
		Connection connection = getInstance();
		Admin admin = connection.getAdmin();
		//创建命名空间描述器
		NamespaceDescriptor ns_mgsr = NamespaceDescriptor
				.create(nameSpace)
				.addConfiguration("creator", "ceshi")
				.addConfiguration("create_time", String.valueOf(System.currentTimeMillis()))
				.build();
		admin.createNamespace(ns_mgsr);
		System.out.println("createNamespace ："+nameSpace);
		//ns_mgsr:content
		//key	info(content)
		createTable(TABLE_CONTENT,1,1, "info");
		//ns_mgsr:relation
		//rowkey:1003   colFamily：attends colName:1001 value:1001  colFamily：fans colName:1001 value:1001
		createTable(TABLE_RELATION,1,1, "attends", "fans");
		//ns_mgsr:inbox
		//rowkey:1003   colFamily：info colName:1001 value:1001_1546918474452
		createTable(TABLE_INBOX,100,100, "info");
		
		admin.close();
		connection.close();
	}
	private void delNameSpace(String nameSpace) throws IOException{
		Connection connection = getInstance();
		Admin admin = connection.getAdmin();
		HTableDescriptor[] list = admin.listTableDescriptorsByNamespace(nameSpace);
		for(HTableDescriptor aa :list) {
			TableName talbe = aa.getTableName();
			admin.disableTable(talbe);
			admin.deleteTable(talbe);
			System.out.println("deleteTable ："+talbe);
		}
		admin.deleteNamespace(nameSpace);
		System.out.println("delNameSpace ："+nameSpace);
		admin.close();
		connection.close();
	}
	
	//创建表
	private void createTable(byte[] tableName, int min, int max, String... cols) throws IOException {
		connection = getInstance();
		Admin admin = connection.getAdmin();
		TableName talbe = TableName.valueOf(tableName);
		if(admin.tableExists(talbe)) {
			System.out.println(talbe+"表已存在！");
		}else {
			//创建表描述器
			HTableDescriptor contentTableDescriptor = new HTableDescriptor(talbe);
			for (String col : cols) {
				//创建列描述器
				HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(col);
				//设置块缓存
				infoColumnDescriptor.setBlockCacheEnabled(true);
				//设置块缓存大小 2M
				infoColumnDescriptor.setBlocksize(2 * 1024 * 1024);
				//设置版本确界
				infoColumnDescriptor.setMinVersions(min);
				infoColumnDescriptor.setMaxVersions(max);
				//将列描述器添加到表描述器中
				contentTableDescriptor.addFamily(infoColumnDescriptor);
			}
			//创建表
			admin.createTable(contentTableDescriptor);
			System.out.println("createTable:"+contentTableDescriptor.getTableName());
		}
		admin.close();
	}
	
	/**
	 * TABLE_CONTENT内容表 rowkey: uid + "_" + ts 
	 * put一个rowkey ，addColumn信息 info ： content ：value
	 * 
	 * TABLE_RELATION关系表
	 * get 一个rowkey（uid） ，addFamily信息   fans
	 * 获得 result，获得fans下的所有列：result.rawCells()，获得列下的value
	 * 
	 * TABLE_INBOX接收表
	 * 对列族fans下所有列的value
	 *  put一个rowkey ，addColumn信息 info ： uid ：ts ： rowkey(uid + "_" + ts)
	 * 
	 * 
	 * @param uid
	 * @param content
	 * @throws IOException
	 */
	public void publishContent(String uid, String content) throws IOException{
		connection = getInstance();
		//内容表添加内容
		Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
		long ts = System.currentTimeMillis();
		String rowkey = uid + "_" + ts;
		Put contentPut = new Put(Bytes.toBytes(rowkey));
		contentPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));
		contentTable.put(contentPut);
		System.out.println("user:"+uid+"=====publishContent:"+content);
		
		//查询关注的fans
		List<byte[]> fansList = new ArrayList<byte[]>();
		Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
		Get get = new Get(Bytes.toBytes(uid));
		get.addFamily(Bytes.toBytes("fans"));
		Result result = relationTable.get(get);
		Cell[] cells = result.rawCells();
		for(Cell cell: cells){
			fansList.add(CellUtil.cloneValue(cell));
		}
		if(fansList.size() <= 0) {
			return;
		}
		
		//记录fans要接受的消息
		Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
		List<Put> puts = new ArrayList<Put>();
		for(byte[] fansRowKey : fansList){
			Put inboxPut = new Put(fansRowKey);
			inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(uid), ts, Bytes.toBytes(rowkey));
			puts.add(inboxPut);
			System.out.println("user:"+fansRowKey+"=====get:"+uid);
		}
		inboxTable.put(puts);
		
		//关闭表与连接器，释放资源
		inboxTable.close();
		relationTable.close();
		contentTable.close();
		connection.close();
	}
	
	/**
	 * ///////////////////////////表结构
	 * TABLE_CONTENT内容表 rowkey: uid + "_" + ts   info ： content ：value
	 * 
	 * TABLE_RELATION关系表 rowkey: otuid  fans : uid : uid
	 * 					  rowkey uid attends : otuid : otuid
	 * 
	 * TABLE_INBOX接收表  rowkey(uid + "_" + ts) info ： uid ：ts ：value(contentkey)
	 * ///////////////////////////表结构
	 * 
	 * @param uid
	 * @param attends
	 * @throws IOException
	 */
	public void addAttends(String uid, String... attends) throws IOException{
		//参数过滤
		if(attends == null || attends.length <= 0 || uid == null) {
			return;
		}
		System.out.println("userid:"+uid +" attend："+attends);
		
		//关联 关系
		connection = getInstance();
		Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
		List<Put> puts = new ArrayList<Put>();
		Put attendPut = new Put(Bytes.toBytes(uid));
		for(String attend: attends){
			//当前用户添加关注人
			attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
			
			//被关注人，添加粉丝
			Put fansPut = new Put(Bytes.toBytes(attend));
			fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
			puts.add(fansPut);
		}
		puts.add(attendPut);
		relationTable.put(puts);
		
		//获得关注之人发布的内容
		Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
		List<byte[]> rowkeys = new ArrayList<byte[]>();
		Scan scan = new Scan();
		for(String attend: attends){
			//关键词过滤
			RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(attend + "_"));
			scan.setFilter(filter);
			ResultScanner resultScanner = contentTable.getScanner(scan);
			Iterator<Result> iterator = resultScanner.iterator();
			while(iterator.hasNext()){
				Result result = iterator.next();
				rowkeys.add(result.getRow());
			}
		}
		if(rowkeys.size() <= 0) {
			return;
		}
		
		//获取关注之人的发布消息
		Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
		Put inboxPut = new Put(Bytes.toBytes(uid));
		for(byte[] rowkey: rowkeys){
			String rowkeyString = Bytes.toString(rowkey);
			String attendId = rowkeyString.split("_")[0];
			String time = rowkeyString.split("_")[1];
			inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendId), Long.valueOf(time), rowkey);
		}
		inboxTable.put(inboxPut);
		
		inboxTable.close();
		contentTable.close();
		relationTable.close();
		connection.close();
	}
	
	public void removeAttends(String uid, String... attends) throws IOException{
		if(attends == null || attends.length <= 0 || uid == null) {
			return;
		}
		connection = getInstance();

		Table relationTable = connection.getTable(TableName.valueOf(TABLE_RELATION));
		Delete attendDelete = new Delete(Bytes.toBytes(uid));
		List<Delete> deletes = new ArrayList<Delete>();
		for(String attend: attends){
			attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
			
			Delete delete = new Delete(Bytes.toBytes(attend));
			delete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes("uid"));
			deletes.add(delete);
		}
		deletes.add(attendDelete);
		relationTable.delete(deletes);
		
		//删收信箱
		Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
		Delete delete = new Delete(Bytes.toBytes(uid));
		for(String attend: attends){
			delete.addColumns(Bytes.toBytes("info"), Bytes.toBytes(attend));
		}
		inboxTable.delete(delete);
		
		//释放资源
		inboxTable.close();
		relationTable.close();
		connection.close();
	}
	
	public void getAttendsContent(String uid) throws IOException{

		connection = getInstance();
		//从收件箱表提取value
		List<byte[]> rowkeys = new ArrayList<byte[]>();
		Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
		Get inboxGet = new Get(Bytes.toBytes(uid));
		inboxGet.addFamily(Bytes.toBytes("info"));
		//每个Cell中存储了100个版本，我们只取出最新的5个版本
		inboxGet.setMaxVersions(5);
		Result inboxResult = inboxTable.get(inboxGet);
		Cell[] inboxCells = inboxResult.rawCells();
		for(Cell cell: inboxCells){
			rowkeys.add(CellUtil.cloneValue(cell));
		}
		
		//根据value从内容表查
		List<Get> contentGets = new ArrayList<Get>();
		Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
		for(byte[] rowkey: rowkeys){
			Get contentGet = new Get(rowkey);
			contentGets.add(contentGet);
		}
		Result[] contentResults = contentTable.get(contentGets);
		for(Result r: contentResults){
			Cell[] cs = r.rawCells();
			for(Cell c: cs){
				String rk = Bytes.toString(r.getRow());
				String userId = rk.split("_")[0];
				String content = Bytes.toString(CellUtil.cloneValue(c));
				long timestamp = Long.valueOf(rk.split("_")[1]);
				System.out.println("userId:"+userId + " timestamp:"+timestamp + " content:"+content);
			}
		}
		
		contentTable.close();
		inboxTable.close();
		connection.close();
	}

    //获取表数据
    public void getAllDealData(byte[] tableName) throws IOException {
    	connection = getInstance();
        Table table= connection.getTable(TableName.valueOf(tableName));
        System.out.println("===================================");
        System.out.println(table);
        Scan scan = new Scan();
        ResultScanner resutScanner = table.getScanner(scan);
        for(Result result: resutScanner){
        	System.out.println("------------------------------------");
//            System.out.println("scan:  " + result);
        	System.out.println("rowkey:" + new String(result.getRow()));
        	 for (Cell cell : result.rawCells()){
                 String colFamily = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                 String colName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
                 String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                 System.out.println("colFamily：" + colFamily+" colName:" + colName + " value:"+value);
        	 }
            
        }
    }
	public static void main(String[] args) throws IOException {
		Business business = new Business();
//		business.initNamespace(NS_MGSR);
		business.delNameSpace(NS_MGSR);
		
//		//发布消息
//		business.publishContent("1001", "1001发的消息1");
//		business.publishContent("1001", "1001发的消息2");
//		business.publishContent("1001", "1001发的消息3");
//		business.publishContent("1001", "1001发的消息4");
//		business.publishContent("1001", "1001发的消息5");
//		business.publishContent("1001", "1001发的消息6");
//		business.publishContent("1002", "1002发的消息");
//		business.publishContent("1003", "1003发的消息");
		
//		business.getAllDealData(TABLE_CONTENT);
//		business.getAllDealData(TABLE_INBOX);
//		business.getAllDealData(TABLE_RELATION);
//		business.getAllDealData("hbase_book".getBytes());
		
//		business.addAttends("1003", "1001");
//		//关注
//		business.addAttends("1001", "1002", "1003");
//		//取消关注
//		business.removeAttends("1001", "1002", "1003");
		//查看信息
//		business.getAttendsContent("1003");
		
	}

}
