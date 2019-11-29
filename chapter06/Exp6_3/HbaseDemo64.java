package hadoop.ch06.v17034460114;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDemo64 {
	
	private static Connection getConn() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "master,node1,node2");
		conf.set("hbase.rootdir", "hdfs://master:9000/hbase");
		conf.set("hbase.cluster.distributed", "true");
		Connection connect = ConnectionFactory.createConnection(conf);
		return connect;	
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		createTable();
		insert();
		get();
		scan();
	}

	private static void createTable() throws Exception {
		// TODO Auto-generated method stub
		//创建Hbase连接
		Connection connect = getConn();
		Admin admin = connect.getAdmin();
		//建库
		TableName tn = TableName.valueOf("emp114");
		//加列族
		String[] family = new String[] {
				"member_id",
				"address",
				"info",
				};
		HTableDescriptor desc = new HTableDescriptor(tn);
		for(int i = 0; i < family.length; i++) {
			desc.addFamily(new HColumnDescriptor(family[i]));
		}
		if(admin.tableExists(tn)) {
			//已存在则提示
			System.out.print("table Exists!");
			System.exit(0);
		}else {
			//不存在则创建
			admin.createTable(desc);
			System.out.print("create table Success!");
		}
	}

	private static void insert() throws Exception {
		// TODO Auto-generated method stub
		Table table = getConn().getTable(TableName.valueOf("emp114"));
		//行键
		Put put = new Put(Bytes.toBytes("Rain"));
		//列族，列名称，值
		put.addColumn(Bytes.toBytes("member_id"), 
				      Bytes.toBytes(""), 
				      Bytes.toBytes("31"));
		put.addColumn(Bytes.toBytes("info"), 
					  Bytes.toBytes("age"), 
					  Bytes.toBytes("28"));
		put.addColumn(Bytes.toBytes("info"), 
				  	  Bytes.toBytes("birthday"), 
				  	  Bytes.toBytes("1990-05-01"));
		put.addColumn(Bytes.toBytes("info"), 
				  	  Bytes.toBytes("industry"), 
				  	  Bytes.toBytes("architect"));
		put.addColumn(Bytes.toBytes("address"), 
				      Bytes.toBytes("city"), 
				      Bytes.toBytes("ShenZhen"));
		put.addColumn(Bytes.toBytes("address"), 
				  	  Bytes.toBytes("country"), 
				  	  Bytes.toBytes("China"));
		//插入
		table.put(put);
		table.close();
		System.out.println("Success!");
	}

	private static void get() throws Exception {
		// TODO Auto-generated method stub
		//get 'emp114','Rain','info'
		Table table = getConn().getTable(TableName.valueOf("emp114"));
		//Get查询
		Get get = new Get(Bytes.toBytes("Rain"));
		//执行查询
		Result record = table.get(get);
		String age = Bytes.toString(record.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
		String birthday = Bytes.toString(record.getValue(Bytes.toBytes("info"), Bytes.toBytes("birthday")));
		String industry = Bytes.toString(record.getValue(Bytes.toBytes("info"), Bytes.toBytes("industry")));
		System.out.println("age=" + age);
		System.out.println("birthday=" + birthday);
		System.out.println("industry=" + industry);
		
	}

	private static void scan() throws Exception {
		// TODO Auto-generated method stub
		//scan 'emp114',{COLUMNS=>'info:birthday'}
		Table table = getConn().getTable(TableName.valueOf("emp114"));
		//创建扫描器
		Scan scanner = new Scan();
		//执行查询
		ResultScanner rs = table.getScanner(scanner);
		for(Result r:rs) {
			String birthday = Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("birthday")));
			System.out.println("birthday" + birthday);
		}
		
	}

}
