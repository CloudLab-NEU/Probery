package com.neu.swc.batchload;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.neu.swc.Probery.Probery;

public class PathLoadJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		String tableName = "Customer";
		
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(PathLoadJob.class);
		
		job.setMapperClass(PathLoadMappper.class);
		job.setReducerClass(PathLoadReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(20);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://cloud0000:9000/user/kimble/probery/path/" + tableName + "/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://cloud0000:9000/user/kimble/output"));
		
		try{
			job.waitForCompletion(true);
		}catch(FileAlreadyExistsException e){
			FileSystem fs = FileSystem.get(URI.create(Probery.uriHDFS),new Configuration());
			fs.delete(new Path("hdfs://cloud0000:9000/user/kimble/output"));
			job.waitForCompletion(true);
			fs.close();
		}
	}
}
