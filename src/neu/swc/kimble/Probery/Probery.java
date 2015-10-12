package neu.swc.kimble.Probery;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import neu.swc.kimble.ETL.FilePartition;
import neu.swc.kimble.ETL.KVPair;
import neu.swc.kimble.ETL.MemoryTable;
import neu.swc.kimble.ETL.SourceTable;
import neu.swc.kimble.MapReduce.QueryJob;
import neu.swc.kimble.SQLLine.QueryPlan;
import neu.swc.kimble.SQLLine.SQLLine;
import neu.swc.kimble.index.Index;
import neu.swc.kimble.probabilityModel.Block;
import neu.swc.kimble.probabilityModel.DataSpace;
import neu.swc.kimble.probabilityModel.DataSpaceFactory;
import neu.swc.kimble.tables.Table;
import neu.swc.kimble.tables.TableList;
import neu.swc.kimble.tables.TableModel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;
import org.dom4j.DocumentException;

public class Probery {

	static final Logger logger = Logger.getLogger(Probery.class);
	public static final String uriHDFS =  "hdfs://Cloud:9000";
	public static final String uriFile =  "file:///";
	
	public static FileSystem fsHDFS,fsFile;
	public static HashMap<Path,SequenceFile.Writer> writterMap = new HashMap<Path,SequenceFile.Writer>();
	
	public static final int cacheNumebr = 20;
	public static boolean isWrite = true;
	
	public static final int rows = 10000;
	public static final String targetDirectoryPath = "/home/kimble/Probery/SourceData/temp/";
	public static String sourceFilePath;
	
	static final String[] attribute_customer = new String[]{"CUSTKEY","NAME","ADDRESS","NATIONKEY","PHONE","ACCTBAL","MKTSEGMENT","COMMENT"};
	static final int[] referAttribute_customer = new int[]{0,2,6};
	
	public static void main(String[] args) throws IOException, DocumentException{
		
		QueryPlan queryPlan;
		int blockNumber;
		DataSpace dataSpace;
		Block block;
		
		TableList tableList = new TableList();
		if(tableList.isNull())
			System.out.println("*******1.Load Data  3.Quit*******");
		else
			System.out.println("*******1.Load Data  2.Probability Query  3.Quit*******");
		
		Index index  = new Index(tableList);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		switch(Integer.parseInt(reader.readLine())){
		case 1:
			logger.info("Load Data");
			
			Probery.fsHDFS = FileSystem.get(URI.create(Probery.uriHDFS),new Configuration());
			Probery.fsFile = FileSystem.get(URI.create(Probery.uriFile),new Configuration());
			String path;
			System.out.println("Please input the file name you want to load:");
			path = reader.readLine();
			while(path.equals(""))
				logger.warn("you need to specify source data path!");
			sourceFilePath =  "/home/kimble/Probery/SourceData/" + path + ".tbl";
			
			Table table = TableModel.getTable(reader);
			tableList.addTable(table);
			logger.info("Load Table Model Successfully");
			reader.close();
			
			//Table table = tableList.getTable("Customer"); //Test
			
			dataSpace = new DataSpace(table.getTableName());
			queryPlan = new QueryPlan();
			queryPlan.setTable(table.getTableName());
			
			SourceTable sourceTable = new SourceTable(attribute_customer,referAttribute_customer);
			int[] referAttributeLocation = sourceTable.getReferAttributeLocation();
			int[] otherAttributeLocaion = sourceTable.getOtherAttributeLocation();
			
			MemoryTable memoryTable = new MemoryTable(sourceTable);
			File[] files = FilePartition.splitDataToSaveFile(rows, sourceFilePath, targetDirectoryPath);
			System.gc();
			logger.info("spliting file finished!");
			
			KVPair<String,String> queryKV;
			KVPair<String,String> kvpairs;
			ArrayList<String[]> memory;	
			int size;
			
			for(int i = 0; i<files.length; i++){
				memory = memoryTable.getMemoryTable(files[i]);
				files[i] = null;
				size = memory.size();
				for(int j=0; j<size;j++){
					queryKV = new KVPair<String,String>();
					kvpairs= new KVPair<String,String>();
					for(int m = 0; m < referAttributeLocation.length;m++){
						kvpairs.put(attribute_customer[referAttributeLocation[m]],memory.get(j)[referAttributeLocation[m]]);
						queryKV.put(attribute_customer[referAttributeLocation[m]],memory.get(j)[referAttributeLocation[m]]);
					}
					for(int n = 0; n<otherAttributeLocaion.length;n++)
						kvpairs.put(attribute_customer[otherAttributeLocaion[n]],memory.get(j)[otherAttributeLocaion[n]]);
					
					queryPlan.setQueryAttribute(queryKV);
					
					index.refresh(tableList);
					blockNumber = index.getBlockNumber(queryPlan);
					
					dataSpace.addBlock(blockNumber);
					block = dataSpace.getBlock(blockNumber);
					block.loadData(kvpairs);
					queryKV.deleteAll();
				}
				memory = null;
				logger.info("customer_temp " + i + " load successfully!");
			}
			for(Block blockTemp : dataSpace.getBlockSet())
				blockTemp.clearCache();
			logger.info("clear cache successfully!");
			
			Set<Path> pathSet = Probery.writterMap.keySet();
			Iterator<Path> iterator = pathSet.iterator();
			while(iterator.hasNext())
				Probery.writterMap.get(iterator.next()).close();
			
			Probery.fsHDFS.copyFromLocalFile(new Path(Probery.uriFile + "/home/kimble/Probery/LocalData/" + table.getTableName()), new Path(Probery.uriHDFS + "/Probery/" + table.getTableName() + "/"));
			logger.info("copy file from local to HDFS successfully!");
			FilePartition.deleteAll(new File("/home/kimble/Probery/LocalData/"));
			
			Probery.fsHDFS.close();
			Probery.fsFile.close();
			DataSpaceFactory.persistenceDataSpace(dataSpace);
			logger.info("Data load successfully!");
			
			FilePartition.deleteAll(new File(targetDirectoryPath));
			break;
		case 2:
			logger.info("Probability Query");
			queryPlan = SQLLine.SQLParser();
			blockNumber = index.getBlockNumber(queryPlan);
			dataSpace = DataSpaceFactory.getDataSpace(queryPlan.getTableName());
			block = dataSpace.getBlock(blockNumber);

			ArrayList<Path> paths = block.getTrunkPath(queryPlan.getRecallProbability());
			Double recallRatio = block.getRecallRatio(queryPlan.getRecallProbability());
			System.err.printf("Recall Ratio: %#.2f\n",recallRatio);
			reader.close();
			QueryJob queryJob = new QueryJob();
			QueryJob.queryAttribute = queryPlan.getQueryAttributeMap();
			QueryJob.select_key = queryPlan.getSelect_key(); 
			queryJob.run(paths);
			break;
		case 3:
			logger.info("Quit");
			System.exit(0);
			break;
		}
	}
}
