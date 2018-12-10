package com.Hbase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class accessTable {
	public static void main(String[] args) throws IOException, Exception{
	//Configuration Hbase
		Configuration conf = HBaseConfiguration.create();
	//Initiallizing htable class
	@SuppressWarnings({"resource", "deprecation"})
	HTable table = new HTable(conf,"TRANSACTIONS");
	//initializing the get class
	Get g = new Get(Bytes.toBytes("4000002"));
	//reading the data
	Result result = table.get(g);
	
	//reading values from result object
	
	byte [] name = result.getValue(Bytes.toBytes("stats"),Bytes.toBytes("username"));
	byte [] txn = result.getValue(Bytes.toBytes("stats"),Bytes.toBytes("count_txn"));
	//print values
	String user = Bytes.toString(name);
	String count = Bytes.toString(txn);
	System.out.println("customer name : " + user + "Number of transactions: " + count);
	
	
	}
	}	
	
	
	
	
	
	
		
		