package com.Hbase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class scanTable {
	public static void main(String[] args) throws IOException,Exception {
	//Configuration Hbase
		Configuration conf = HBaseConfiguration.create();
	//Initiallizing htable class
	@SuppressWarnings({"deprecation","resource"})
	HTable table = new HTable(conf,"TRANSACTIONS");
	//initializing the scan class
	Scan scan = new Scan();
	//Scanning  the required columns
	scan.addColumn(Bytes.toBytes("stats"),Bytes.toBytes("count_txn"));
	scan.addColumn(Bytes.toBytes("stats"),Bytes.toBytes("username"));
	//Getting the result
	ResultScanner scanner = table.getScanner(scan);
	//reading from the scan results
	for(Result result = scanner.next();result != null; result = scanner.next()){
	//assign row values in variable row
	String row = Bytes.toString(result.getRow());
	//assign column username values in name
	String name = Bytes.toString(result.getValue("stats".getBytes(),"username".getBytes()));
	//assign column count_txn values in count
	String count = Bytes.toString(result.getValue("stats".getBytes(),"count_txn".getBytes()));
	System.out.println(row + "," + name + "," +  count );
	
	
	//closing the scanner
	scanner.close();
	}
	}	
	}
	
	
	
	
	
		
		