package neu.swc.kimble.MapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import neu.swc.kimble.ETL.KVPair;

public class DataQuery extends Mapper<Text, Text, Text, Text>{
	
	KVPair<String,String> kvpair;
	String strKey, strValue;
	
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
				for(String str:QueryJob.select_key){
					rKey.set(str);
					rValue.set(kvpair.get(str));
					context.write(rKey,rValue);
				}
			}
			kvpair.deleteAll();
		}
		else
			kvpair.put(strKey, strValue);
	}
}
