package com.neu.swc.batchload;

import java.io.File;
//import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.swc.Probery.Probery;
import com.neu.swc.probabilityModel.Block;
import com.neu.swc.probabilityModel.PlacingProbability;

public class BatchImportReducer extends Reducer<LongWritable, Text, Text, Text> {
	
	public final String[] attribute_Part = new String[]{"PARTKEY","NAME","MFGR","BRAND","TYPE","SIZE","CONTAINER","RETAILPRICE","COMMENT"};
	public final String[] attribute_Customer = new String[]{"CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMENT","COMMENT"};
	
	public String dataSpaceName;
	
	public List<Block> blocks;
	public List<Long> groups;
	public long group;
	
	Configuration conf;
	FileSystem fsFile;
	SequenceFile.Writer[] writerArray;
	
	protected void setup(Context context) throws IOException{
		dataSpaceName = context.getConfiguration().get("tablename");
		blocks = new ArrayList<Block>();
		this.groups = new ArrayList<Long>();
		this.group = -1;
		
		conf = new Configuration();
		fsFile = FileSystem.get(URI.create(Probery.uriFile), conf);
		fsFile.setVerifyChecksum(false);
	}
	
	public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
		if(key.get() != this.group){
			writerArray = new SequenceFile.Writer[PlacingProbability.getInstance().getProbabilityArrayLength()];
			this.group = key.get();
			this.groups.add(group);
		}
		for(Text val : value){
			Block block =null;
			String[] values = val.toString().split("\\|");
			int blockNumber = Integer.parseInt(values[values.length-1]);
			for(Block blockTemp : blocks){
				if(blockTemp.getBlockNumber() == blockNumber){
					block = blockTemp;
					break;
				}
			}
			if(block == null){
				block = new Block(blockNumber, dataSpaceName);
				blocks.add(block);
			}
			loadData(block, values);
		}
		
		for(Block blockTemp: blocks){
			Text outputKeys = new Text(Integer.toString(blockTemp.getBlockNumber()));
			Text outputValues = new Text();
			ArrayList<Integer> placingNumber = blockTemp.getPlacingNumber();
			StringBuilder string = new StringBuilder();
			if(placingNumber.size() != 0)
			{
				for(int number:placingNumber)
				{
					string.append(Integer.toString(number));
					string.append("\t");
				}
			}
			outputValues.set(string.toString());
			context.write(outputKeys, outputValues);
		}
		blocks.clear();
	}
	
	
	@SuppressWarnings("deprecation")
	public void loadData(Block block, String[] values) throws IOException{
		String filePath;
		int fileNumber;
		boolean flag = false;
		Text key = new Text();
		Text value = new Text();	
		
		fileNumber = ((block.placingNumber.size() == 0)? 1 : block.placingNumber.size());
		
		try{
			flag = block.isAddingTrunk(writerArray);
			
			int number = block.bucketNumberToPlacing();
			filePath = block.getFilePath(number);
			if(flag){
				filePath = filePath + Integer.toString(++fileNumber);	
				for(SequenceFile.Writer writer:writerArray)
					writer.close();
				writerArray = new SequenceFile.Writer[PlacingProbability.getInstance().getProbabilityArrayLength()];
			}
			else
				filePath = filePath + Integer.toString(fileNumber);
			
			if(writerArray[number-1] == null)
				writerArray[number-1] = SequenceFile.createWriter(fsFile,conf,new Path(filePath),key.getClass(),value.getClass(),CompressionType.BLOCK);
			
			if(block.placingNumber.size()+1 == fileNumber)
				block.placingNumber.add(0);
				
			for(int i=0; i<values.length-1; i++){
				key.set(attribute_Customer[i]);
				value.set(values[i]);
				writerArray[number-1].append(key, value);
			}
			key.set("&");
			value.set("&");
			writerArray[number-1].append(key, value);

			block.placingNumber.set(block.placingNumber.size()-1, block.placingNumber.get(block.placingNumber.size()-1)+1);

		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		for(SequenceFile.Writer writer:writerArray)
			writer.close();
		fsFile.close();
		
		File dir;
		File[] files;
		String fileName;
		FileSystem fsHDFS = FileSystem.get(URI.create(Probery.uriHDFS), new Configuration());
		fsHDFS.setVerifyChecksum(false);
		for(Long groupNumber:this.groups){
			for(int i=1; i<=PlacingProbability.getInstance().getProbabilityArrayLength(); i++){
				dir = new File("/home/kimble/software/probery/data/" + dataSpaceName + "/" + Long.toString(groupNumber) +"/" + Integer.toString(i));
				files = dir.listFiles();
				for(File file:files){
					fileName = file.getName();
					if(fileName.contains("crc"))
						file.delete();
				}
			}
			fsHDFS.mkdirs(new Path("/home/kimble/software/probery/data/" + dataSpaceName + "/" + Long.toString(groupNumber)));
			for(int i=1; i<=PlacingProbability.getInstance().getProbabilityArrayLength(); i++){
				fsHDFS.mkdirs(new Path("/home/kimble/software/probery/data/" + dataSpaceName + "/" + Long.toString(groupNumber) + "/" +  Integer.toString(i)));
				fsHDFS.copyFromLocalFile(new Path("/home/kimble/software/probery/data/" + dataSpaceName + "/" + Long.toString(groupNumber) + "/" + Integer.toString(i) + "/"), new Path(Probery.uriHDFS + "/user/kimble/probery/" + dataSpaceName + "/" + Long.toString(groupNumber) + "/" +  Integer.toString(i) + "/"));
			}
		}
		fsHDFS.close();
	}
	
	/*for(int i=1; i<=PlacingProbability.getInstance().getProbabilityArrayLength(); i++){
	dir = new File("/home/kimble/software/probery/data/" + dataSpaceName + "/" + Long.toString(groupNumber) +"/" + Integer.toString(i));
	files = dir.listFiles();
	for(File file:files){
		fileName = file.getName();
		if(fileName.contains("crc"))
			file.delete();
		else{
			split = fileName.split("-");
			file.renameTo(new File(file.getParent() + "/" + split[0]));
		}
	}
}*/
}
