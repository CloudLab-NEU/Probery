package com.neu.swc.query;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.swc.etl.KVPair;

public class DataQuery extends Mapper<Text, Text, Text, Text>{
	
	private KVPair<String,String> kvpair = new KVPair<String,String>();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		
		KVPair<String,String> queryAttribute = new KVPair<String,String>();
		String strKey, strValue,temp;
		Iterator<String> iterator;
		Text rKey = new Text();
		Text rValue = new Text(); 
		boolean flag = true;
		
		strKey =key.toString();
		strValue = value.toString();
		
		if(strKey.equals("&")){
			for(int i=0; i<Integer.parseInt(context.getConfiguration().get("querySize")); i++)
				queryAttribute.put(context.getConfiguration().get("queryKey" + i), context.getConfiguration().get("queryValue" + i));
		
			iterator = kvpair.iterator();
			while(iterator.hasNext() & flag){
					temp = iterator.next();
					if(queryAttribute.containsKey(temp)){
						if(!queryAttribute.get(temp).equals(kvpair.getCorrespondingValue()))
							flag = false;
					}
			}
			if(flag){
				for(int i=0; i<Integer.parseInt(context.getConfiguration().get("selectKeySize"));i++){
					rKey.set(context.getConfiguration().get("selectKey" + i));
					rValue.set(kvpair.get(context.getConfiguration().get("selectKey" + i)));
					context.write(rKey,rValue);
				}
			}
			kvpair.deleteAll();
		}
		else
			kvpair.put(strKey, strValue);
	}
}
