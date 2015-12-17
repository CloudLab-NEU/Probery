package com.neu.swc.batchload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.dom4j.DocumentException;


public class ProberyLoad {
	

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
		@SuppressWarnings("deprecation")
		Job job = new Job(configuration);
		job.setJarByClass(ProberyLoad.class);
		
		job.setMapperClass(BatchImportMapper.class);
		job.setReducerClass(BatchImportReducer.class);
		
		job.setNumReduceTasks(10);
		
		
		FileInputFormat.addInputPaths(job,"hdfs://cloud0000:9000/user/kimble/sourcedata/customer-10G.tbl");
		FileOutputFormat.setOutputPath(job, new Path("hdfs://cloud0000:9000/user/kimble/probery/metadata/" + tableName));
		
		job.waitForCompletion(true);
	}
	

}
