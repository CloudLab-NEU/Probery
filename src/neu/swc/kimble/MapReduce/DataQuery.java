package neu.swc.kimble.MapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import neu.swc.kimble.ETL.KVPair;

public class DataQuery extends Mapper<Text, Text, Text, Text>{
	
	KVPair<String,String> kvpair;
	TreeMap<String,String> treeMap;
	String strKey, strValue;
	//private StringTokenizer strTokenizer1, strTokenizer2;
	//private boolean flag;
	
	/*
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		strTokenizer1 = new StringTokenizer(key.toString(),"\t");
		flag = true;
		while(flag){
			while(strTokenizer1.hasMoreTokens()){
				strTokenizer2 = new StringTokenizer(strTokenizer1.nextToken(),"|");
				strTokenizer2.nextToken();
				if(!QueryJob.queryAttribute.get(QueryJob.queryAttribute.firstKey()).equals(strTokenizer2.nextToken()))
					flag = false;
			}
		}
		
		Text rKey = new Text();
		Text rValue = new Text();
		String str;
		if(flag){
			strTokenizer1 = new StringTokenizer(value.toString(),"\t");
			while(strTokenizer1.hasMoreTokens()){
				strTokenizer2 = new StringTokenizer(strTokenizer1.nextToken(),"|");
				str = strTokenizer2.nextToken();
				if(QueryJob.select_key.contains(str)){
					rKey.set(str);
					rValue.set(strTokenizer2.nextToken());
					context.write(rKey, rValue);
				}	
			}
		}
	}*/
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		kvpair = new KVPair<String,String>();
		boolean flag = true;
		Iterator<String> iterator;
		Text rKey = new Text();
		Text rValue = new Text();
		
		strKey =key.toString();
		strValue = value.toString();
		
		if(strKey.equals("&")){
			iterator = kvpair.iterator();
			while(iterator.hasNext() & flag){
				if(!QueryJob.queryAttribute.get(iterator.next()).equals(kvpair.getCorrespondingValue()))
					flag = false;
			}
			
			if(flag){
				treeMap = kvpair.getTreeMap();
				for(String str : QueryJob.select_key){
					rKey.set(str);
					rValue.set(treeMap.get(str));
					context.write(rKey,rValue);
				}
			}
			kvpair.deleteAll();
		}
		else
			kvpair.put(strKey, strValue);
	}
}
