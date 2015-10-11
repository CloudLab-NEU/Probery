package neu.swc.kimble.ETL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class MemoryTable {
	
	private ArrayList<String[]> memoryTable;
	private SourceTable sourceTable;
	
	public MemoryTable(SourceTable sourceTable){
		this.sourceTable = sourceTable;
		this.memoryTable = new ArrayList<String[]>();
	}
	
	public ArrayList<String[]>  getMemoryTable(File file) throws IOException{
		String string;
		String[] str = new String[this.sourceTable.getSize()];
		int i = 0;
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		string = reader.readLine();
		while(string != null){
			StringTokenizer tokenizer = new StringTokenizer(string,"|");
			while(tokenizer.hasMoreTokens()){
				str[i] =tokenizer.nextToken();
				i++;
			}
			this.memoryTable.add(str);
			string = reader.readLine();
			i = 0;
			str = new String[this.sourceTable.getSize()];
		}
		reader.close();
		return this.memoryTable;
	}
}
