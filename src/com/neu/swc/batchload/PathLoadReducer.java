package com.neu.swc.batchload;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.swc.Probery.Probery;

public class PathLoadReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

	public final String[] Customer = new String[]{"CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMENT","COMMENT"};
	
	public String filePath;
	public Configuration conf;
	public FileSystem fs;
	public SequenceFile.Writer writer;
	
	public Text k,v;
	public String[] values;
	
	public void setup(Context context) throws IOException{
		conf = new Configuration();
		fs = FileSystem.get(URI.create(Probery.uriHDFS), conf);
		k = new Text();
		v = new Text();
	}
	@SuppressWarnings("deprecation")
	public void reduce(Text key, Iterable<Text> value, Context context) throws IOException{
		filePath = key.toString() + "/1";
		writer = SequenceFile.createWriter(fs, conf, new Path(filePath), k.getClass(), v.getClass(),CompressionType.BLOCK);
		for(Text line:value){
			values = line.toString().split("\\|");
			for(int i=0; i<Customer.length; i++){
				k.set(Customer[i]);
				v.set(values[i]);
				writer.append(k, v);
			}
			k.set("&");
			v.set("&");
			writer.append(k, v);
		}
		writer.close();
	}
}
