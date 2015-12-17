package com.neu.swc.query;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.swc.ETL.KVPair;
import com.neu.swc.Probery.Probery;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class QueryJob {
	  
	private Configuration conf;  
	private KVPair<String,String> queryAttribute;
	private ArrayList<String> select_key;
	
	public QueryJob(KVPair<String,String> queryAttribute,ArrayList<String> select_key){
		this.conf = new Configuration();
		this.queryAttribute = queryAttribute;
		this.select_key = select_key;
	}

	@SuppressWarnings("deprecation")
	public void run(ArrayList<Path> paths) throws IOException, ClassNotFoundException, InterruptedException{
			//conf.setBoolean("mapreduce.map.output.compress", false); 
			
			Job job = new Job(this.conf);
			job.setJobName("Probery");
			job.setJarByClass(Probery.class);
			job.setMapperClass(DataQuery.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			for(Path path:paths)
				FileInputFormat.addInputPath(job, path);
			
			for(int i=0; i<this.select_key.size();i++)
				job.getConfiguration().set("selectKey"+i, this.select_key.get(i));
			job.getConfiguration().set("selectKeySize",Integer.toString(this.select_key.size()));
			
			for(int j=0; j<this.queryAttribute.size();j++){
				job.getConfiguration().set("queryKey" + j, this.queryAttribute.getK(j));
				job.getConfiguration().set("queryValue" + j, this.queryAttribute.getV(j));
			}
			job.getConfiguration().set("querySize",Integer.toString(this.queryAttribute.size()));
			
			FileOutputFormat.setOutputPath(job,new Path(Probery.uriHDFS + "/user/kimble/probery/result/"));
			
			try{
				job.waitForCompletion(true);
			}catch(FileAlreadyExistsException e){
				FileSystem fs = FileSystem.get(URI.create(Probery.uriHDFS),new Configuration());
				fs.delete(new Path(Probery.uriHDFS + "/user/kimble/probery/result/"));
				job.waitForCompletion(true);
			}
	}
}
