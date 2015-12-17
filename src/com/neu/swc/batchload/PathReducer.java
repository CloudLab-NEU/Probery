package com.neu.swc.batchload;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.swc.probabilityModel.Block;

public class PathReducer extends Reducer<IntWritable, Text, Text, Text>{
	
	public final String[] attribute_Part = new String[]{"PARTKEY","NAME","MFGR","BRAND","TYPE","SIZE","CONTAINER","RETAILPRICE","COMMENT"};
	public final String[] attribute_Customer = new String[]{"CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMENT","COMMENT"};
	
	public String dataSpaceName;
	public Block block;
	public String filePath;
	public int blockNumber;
	public Text rkey;
	
	
	//public int[][] placingNumber;
	//public int groupNumber;
	//public int bucketNumber;
	//public final int limit = 960000;
	
	protected void setup(Context context){
		dataSpaceName = context.getConfiguration().get("tablename");
		//placingNumber = new int[PlacingProbability.getInstance().getProbabilityArrayLength()][];
	}
	
	public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		blockNumber = key.get();
		
		// recording the placing number of each group to split trunk
		
		block = new Block(blockNumber,dataSpaceName);
		//groupNumber = block.getBucketGroupNumber();
		//bucketNumber = block.bucketNumberToPlacing();
		//placingNumber[bucketNumber-1][groupNumber] += placingNumber[bucketNumber-1][groupNumber];
		for(Text val : value){
			filePath = block.getFilePath();
			rkey.set(filePath);
			context.write(rkey, val);
		}
	}
	
	protected void cleanup(Context context){
		
	}
}
