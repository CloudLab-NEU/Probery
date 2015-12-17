package com.neu.swc.probabilityModel;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class DataSpaceFactory {
	
	private static String url = "/home/kimble/Probery/MetaData/PlacingNumber/";
	private final static Logger logger = Logger.getLogger(DataSpaceFactory.class);
	
	public static DataSpace getDataSpace(String dataSpaceName){
		DataSpace dataSpace = initializeDataSpace(dataSpaceName);
		return dataSpace;
	}
	
	public static void persistenceDataSpace(DataSpace dataSpace) throws IOException{
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(url + dataSpace.getName())));
		
		ArrayList<Integer> placingNumber;
		for(Block block : dataSpace.getBlockSet()){
			StringBuilder string = new StringBuilder();
			string.append(Integer.toString(block.getBlockNumber()));
			
			placingNumber = block.getPlacingNumber();
			if(placingNumber.size() != 0)
			{
				for(int number:placingNumber)
				{
					string.append("\t");
					string.append(Integer.toString(number));
				}
			}
			string.append("\n");
			writer.write(string.toString());
		}
		writer.flush();
		writer.close();
		logger.info("meta data persistence success");
	}
	
	private static DataSpace initializeDataSpace(String dataSpaceName){
		BufferedReader reader;
		String string;
		Block block;
		StringTokenizer tokenizer;
		ArrayList<Integer> placingNumber;
		DataSpace dataSpace = new DataSpace(dataSpaceName);
		try {
			reader = new BufferedReader(new FileReader(new File(url + dataSpaceName)));
			string = reader.readLine();
			while(string != null){
				tokenizer = new StringTokenizer(string,"\t");
				block = new Block(Integer.parseInt(tokenizer.nextToken()),dataSpaceName);
				placingNumber = block.getPlacingNumber();
				while(tokenizer.hasMoreTokens())
					placingNumber.add(Integer.parseInt(tokenizer.nextToken()));
				dataSpace.addBlock(block);
				string = reader.readLine();
			}
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Meta data read exception:  " + e.getMessage());
			return null;
		}
		logger.info("meta data reading successful!");
		return dataSpace;
	}
}
