package com.neu.swc.batchload;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PathLoadMappper extends Mapper<LongWritable, Text, Text, Text> {

	public Text rkey = new Text();
	public Text rvalue = new Text();
	public String[] values;

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		values = value.toString().split("\t");
		rkey.set(values[0]);
		rvalue.set(values[1]);
		context.write(rkey, rvalue);
	}
}
