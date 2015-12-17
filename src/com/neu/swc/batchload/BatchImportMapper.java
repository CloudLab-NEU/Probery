package com.neu.swc.batchload;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.swc.tables.Table;
import com.neu.swc.tables.TableList;

import com.neu.swc.ETL.KVPair;
import com.neu.swc.ETL.SourceTable;
import com.neu.swc.SQLLine.QueryPlan;
import com.neu.swc.index.Index;
import com.neu.swc.probabilityModel.Block;

public class BatchImportMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public TableList tableList;
	public Index index;
	public Table table;
	public QueryPlan queryPlan;
	public int[] referAttributeLocation;
	public int[] otherAttributeLocaion;
	
	
	public final String[] attribute_Part = new String[]{"PARTKEY","NAME","MFGR","BRAND","TYPE","SIZE","CONTAINER","RETAILPRICE","COMMENT"};
	public  final int[] referAttribute_Part = new int[]{4,5,6,7};
	
	public final String[] attribute_Customer = new String[]{"CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMENT","COMMENT"};
	public final int[] referAttribute_Customer = new int[]{0,2,6};
	
	protected void setup(Context context) throws IOException{
		
		SourceTable sourceTable = new SourceTable(attribute_Customer,referAttribute_Customer);
		referAttributeLocation = sourceTable.getReferAttributeLocation();
		otherAttributeLocaion = sourceTable.getOtherAttributeLocation();
		
		tableList = new TableList();
		table = tableList.getTable(context.getConfiguration().get("tablename"));
		index = new Index(tableList);
		
		queryPlan = new QueryPlan();
		queryPlan.setTable(table.getTableName());
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String[] values = value.toString().split("\\|");
		
		KVPair<String,String> queryKV = new KVPair<String, String>();
		for(int m = 0; m < referAttributeLocation.length;m++)
			queryKV.put(attribute_Customer[referAttributeLocation[m]],values[referAttributeLocation[m]]);
		
		queryPlan.setQueryAttribute(queryKV);
		
		int blockNumber = index.getBlockNumber(queryPlan);
		Block block = new Block(blockNumber, table.getTableName());
		int groupNumber = block.getBucketGroupNumber();
		context.write(new IntWritable(groupNumber), new Text(value.toString() + Integer.toString(blockNumber)));
	}
	
	protected void cleanup(Context context){
		
	}
}
