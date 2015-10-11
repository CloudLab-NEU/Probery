package neu.swc.kimble.probabilityModel;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import neu.swc.kimble.ETL.KVPair;
import neu.swc.kimble.Probery.Probery;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

public class Block {

	private int blockNumber;
	private double[] placingProbability;
	private ArrayList<Integer> placingNumber;
	private String dataSpace;
	
	private int counter;
	private KVPair<String,String>[] cachePair;
	
	private final Logger logger = Logger.getLogger("AttributeRegion.class");
	
	@SuppressWarnings("unchecked")
	public Block(int blockNumber, String dataSpace){
		this.blockNumber = blockNumber;
		this.dataSpace = dataSpace;
		this.placingProbability = PlacingProbability.getInstance().getPlacingProbability(this.blockNumber);	
		this.placingNumber = new ArrayList<Integer>();
		this.counter = 0;
		this.cachePair  = new KVPair[Probery.cacheNumebr]; 
	}
	
	public int getBlockNumber(){
		return this.blockNumber;
	}
	
	public ArrayList<Path> getTrunkPath(double recallProbability){
		ArrayList<Path> paths = new ArrayList<Path>();
		String filePath = "/Probery/" + this.dataSpace + "/"+ Integer.toString(this.getBucketGroupNumber()) + "/";
		if(this.placingNumber.size() == 0)
			return null;
		else if(recallProbability == 1){
			for(int i=0; i<this.placingNumber.size(); i++){
				for(int j=0; j<this.placingProbability.length; j++){
						paths.add(new Path(filePath + Integer.toString(j+1) +"/"+ Integer.toString(i+1)));
				}
			}
			return paths;
		}
		
		double[][] probability = this.queryData(recallProbability);
		for(int i=0; i<this.placingNumber.size(); i++){
			for(int j=0; j<this.placingProbability.length; j++){
				System.err.println(probability[i][j]);
				if(probability[i][j] < 1)
					paths.add(new Path(filePath + Integer.toString(j+1) +"/"+ Integer.toString(i+1)));
			}
		}
		return paths;
	}
	
	public double getRecallRatio(double recallProbability){
		if(this.placingNumber.size() == 0)
			return 0;
		else if(recallProbability == 1)
			return 1;
		double sumSelect = 0, sumAll = 0, placingNumber = 0;
		double[][] probability = this.queryData(recallProbability);
		
		for(int i=0; i<this.placingNumber.size(); i++){
			placingNumber = this.placingNumber.get(i);
			sumAll += placingNumber;
			for(int j=0; j<this.placingProbability.length; j++){
				if(probability[i][j] < 1)
					sumSelect += placingNumber * this.placingProbability[j];
			}
		}
		return sumSelect/sumAll;
	}
	
	private double[][] queryData(double recallProbability){
		double sum = 0;
		double probability[][] = new double[this.placingNumber.size()][this.placingProbability.length];
		
		for(int i=0; i<this.placingNumber.size(); i++){
			for(int j=0; j<this.placingProbability.length; j++){
				probability[i][j] = 1- Math.pow(1-this.placingProbability[j], this.placingNumber.get(i));
				sum += probability[i][j];
			}
		}
		
		for(int i=0; i<this.placingNumber.size(); i++){
			for(int j=0; j<this.placingProbability.length; j++){
				probability[i][j] = 1 - probability[i][j]/sum;
				if(probability[i][j] <= recallProbability)
					probability[i][j] = 0;
			}
		}
		
		double maxValue = 1;
		double temp = 1;
		while(maxValue != 0){
			maxValue = this.getMaxValue(probability);
			temp *= maxValue;
			if(temp < recallProbability)
				maxValue = 0;
		}
		return probability;
	}
	
	private double getMaxValue(double[][] probability){
		int tempRow = 0;
		int tempColumn = 0;
		double maxValue = 0;
		for(int i=0; i<probability.length; i++){
			for(int j=0; j<this.placingProbability.length; j++){
				if(probability[i][j] < 1 & probability[i][j] > 0){
					if(maxValue < probability[i][j]){
						maxValue = probability[i][j];
						tempRow = i;
						tempColumn = j;
					}
				}
			}
		}
		probability[tempRow][tempColumn] = 1;
		return maxValue;
	}
	
	public boolean loadData(KVPair<String,String> kvpairs){
		boolean flag = true;
		if(this.counter == Probery.cacheNumebr){
			this.counter = 0;
			while(flag){
				if(Probery.isWrite){
					Probery.isWrite = false;
					if(this.loadCacheData()){
						Probery.isWrite = true;
						flag = false;
					}	
				}
			}
		}
		this.cachePair[this.counter] = kvpairs;
		++this.counter;
		return true;
	}
	
	public boolean clearCache(){
		boolean flag = true;
		while(flag){
			if(Probery.isWrite){
				Probery.isWrite = false;
				if(this.loadCacheData()){
					Probery.isWrite = true;
					flag = false;
				}	
			}
		}
		return true;
	}
	
	@SuppressWarnings({ "deprecation" })
	private boolean loadCacheData(){
		String filePath;
		Path path;
		int fileNumber;
		boolean flag = false;
		Iterator<String> iterator;
		SequenceFile.Writer writer = null;
		Text key = new Text();
		Text value = new Text();
		
		fileNumber = ((this.placingNumber.size() == 0)? 1 : this.placingNumber.size());
		
		try{
			flag = this.isExistingTrunk(fileNumber);
			for(int i=0; i<this.cachePair.length; i++){
				if(cachePair[i] == null)
					return true;
				
				filePath = this.getFilePath();
				if(flag){
					path = new Path(filePath + Integer.toString(++fileNumber));	
					flag = false;
				}
				else
					path = new Path(filePath + Integer.toString(fileNumber));
				
				if(!Probery.writterMap.containsKey(path)){
					Probery.fsFile.create(path);
					writer = SequenceFile.createWriter(Probery.fsFile, new Configuration(), path,key.getClass(),value.getClass(),CompressionType.RECORD);
					Probery.writterMap.put(path, writer);
				}
				else
					writer = Probery.writterMap.get(path);
				
				if(this.placingNumber.size()+1 == fileNumber)
					this.placingNumber.add(0);
				
				iterator = cachePair[i].iterator();
				while(iterator.hasNext()){
					key.set(iterator.next().toString());
					value.set(cachePair[i].getCorrespondingValue().toString());
					writer.append(key, value);
				}
				this.placingNumber.set(this.placingNumber.size()-1, this.placingNumber.get(this.placingNumber.size()-1)+1);
				key.set("&");
				value.set("&");
				writer.append(key, value);
				this.cachePair[i].deleteAll();
				this.cachePair[i] = null;
				writer.sync();
			}
			return true;
		}catch(Exception e){
			logger.error("HDFS Error: Data load exception");
			e.printStackTrace();
			return false;
		}
}
	
	
	private boolean isExistingTrunk(int fileNumber) throws IOException{
		File file;
		String filePath = "/home/kimble/Probery/LocalData/" + this.dataSpace + "/" + Integer.toString(this.getBucketGroupNumber()) + "/";
		for(int i = 1; i<= this.placingProbability.length; i++){
			filePath += Integer.toString(i) + "/" + Integer.toString(fileNumber);
			file = new File(filePath);
			if(file.exists()){
				if(file.length() > 67100000)
					return true;
			}
		}
		return false;
	}
	
	private String getFilePath(){
		String filePath = "/home/kimble/Probery/LocalData/" + this.dataSpace + "/" + Integer.toString(this.getBucketGroupNumber()) + "/" + Integer.toString(this.bucketNumberToPlacing()) + "/";
		return filePath;
	}
	
	private int getBucketGroupNumber(){
		return this.blockNumber/PlacingProbability.getInstance().getBucketNumberPerGroup() + 1;
	}
	
	private int bucketNumberToPlacing(){
		int randomNumber;
		double probabilitySum = 0;
		Random random = new Random();
		randomNumber = random.nextInt(1000) + 1;
		for(int i = 0; i < this.placingProbability.length; i++){
			probabilitySum += this.placingProbability[i];
			if((double)randomNumber/1000 <= probabilitySum)
				return i+1;
		}
		return 0;
	}
	
	public ArrayList<Integer> getPlacingNumber() {
		return this.placingNumber;
	}
}
