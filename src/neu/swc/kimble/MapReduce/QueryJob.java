package neu.swc.kimble.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import neu.swc.kimble.Probery.Probery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

public class QueryJob {
	
	protected static SequenceFile.Reader reader = null;  
	protected static Configuration conf = new Configuration();  
	public static TreeMap<String,String> queryAttribute;
	public static ArrayList<String> select_key;

	public void run(ArrayList<Path> paths){
		try {
			conf.set("mapred.max.split.size", "104857600");
			conf.setBoolean("mapred.output.compress", false); 
			
			@SuppressWarnings("deprecation")
			Job job = new Job(QueryJob.conf);
			job.setJobName("Probery");
			job.setJarByClass(Probery.class);
			job.setMapperClass(DataQuery.class);
			job.setInputFormatClass(CombineSequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			for(Path path:paths)
				FileInputFormat.addInputPath(job, path);
			
			FileOutputFormat.setOutputPath(job,new Path(Probery.uriFile + "/home/kimble/Probery/output/"));
			
	        System.exit(job.waitForCompletion(true)?0:1);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
