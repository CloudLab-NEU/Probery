package com.neu.swc.batchload;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dom4j.DocumentException;

import com.neu.swc.Probery.Probery;

public class PathJob {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, DocumentException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		/*TableList tableList = new TableList();
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		Table table = TableModel.getTable(reader);
		tableList.addTable(table);
		String tableName = table.getTableName();*/
		
		String tableName = "Customer";
		
		Configuration configuration  = new Configuration();
		configuration.set("tablename", tableName);
		Job job = new Job(configuration);
		job.setJarByClass(PathJob.class);
		
		job.setMapperClass(PathMapper.class);
		job.setReducerClass(PathReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setNumReduceTasks(20);
		
		
		FileInputFormat.addInputPaths(job,"hdfs://cloud0000:9000/user/kimble/sourcedata/customer-10G.tbl");
		FileOutputFormat.setOutputPath(job, new Path("hdfs://cloud0000:9000/user/kimble/probery/path/" + tableName));
		
		try{
			job.waitForCompletion(true);
		}catch(FileAlreadyExistsException e){
			FileSystem fs = FileSystem.get(URI.create(Probery.uriHDFS),new Configuration());
			fs.delete(new Path("hdfs://cloud0000:9000/user/kimble/probery/path/" + tableName));
			job.waitForCompletion(true);
		}
	}
}
