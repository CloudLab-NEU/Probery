package neu.swc.kimble.SQLLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import neu.swc.kimble.ETL.KVPair;

/*
 * SQL-Like clause programmer:
 * 
 * select attri_A,attri_B,avg(attri_C)
 * from tableName
 * where queryAttri_A=w,queryAttri_B=m
 * rp 0.8
 */

public class SQLLine {
	
	static final Logger logger = Logger.getLogger(SQLLine.class);
	
	private static String readSQLLine(){
		BufferedReader sqlLine = new BufferedReader(new InputStreamReader(System.in));
		String string;
		StringBuilder stringBuilder = new StringBuilder();
		System.out.println("Please input query statement: ");
		for(int i=0; i<4; i++){
			try {
				string = sqlLine.readLine();
				if(string.equals(""))
				{
					logger.warn("SQL Error, Please input again!");			
					i--;
				}
				else
				{
					stringBuilder.append(string);
					stringBuilder.append("\n");
				}			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("io exception", e);
			}
		}
		return stringBuilder.toString();
	}
	
	private static QueryPlan parseSQLLine(String SQLLine){
		
		ArrayList<String> clauses = new ArrayList<String>();
		QueryPlan queryPlan = new QueryPlan();
		
		StringTokenizer tokenizer = new StringTokenizer(SQLLine,"\n");
		StringTokenizer sub_tokenizer;
		ArrayList<String> sub_clause = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()){
			sub_tokenizer = new StringTokenizer(tokenizer.nextToken(), " ");
			clauses.add(sub_tokenizer.nextToken());
			sub_clause.add(sub_tokenizer.nextToken());
		}
			
		for(String clause : clauses){
			StringTokenizer comma_tokenizer;
			switch(clause){
			case "select":{
				comma_tokenizer = new StringTokenizer(sub_clause.get(0),",");
				
				ArrayList<String> select_key = new ArrayList<String>();
				//ArrayList<String> key_function = new ArrayList<String>();
				KVPair<String,String> key_function = new KVPair<String,String>();
				
				while(comma_tokenizer.hasMoreTokens()){
					String attribute = comma_tokenizer.nextToken();
					if(attribute.contains("avg") || attribute.contains("min") || attribute.contains("max")
							|| attribute.contains("sum") || attribute.contains("count")){
						int bracket_start = attribute.indexOf("(");
						int bracket_end = attribute.indexOf(")");
						String function = attribute.substring(0,bracket_start);
						attribute = attribute.substring(bracket_start+1,bracket_end);
						key_function.put(function, attribute);
					}
					select_key.add(attribute);
				}
				queryPlan.setAggregation(key_function);
				queryPlan.setSelect_key(select_key);
				break;
			}	
			case "from":{
				queryPlan.setTable(sub_clause.get(1));
				break;
			}
			case "where":{
				comma_tokenizer = new StringTokenizer(sub_clause.get(2),",");
				KVPair<String,String> queryAttribute = new KVPair<String,String>();
				StringTokenizer equal_tokenizer;
				while(comma_tokenizer.hasMoreTokens()){
					equal_tokenizer = new StringTokenizer(comma_tokenizer.nextToken(),"=");
					queryAttribute.put(equal_tokenizer.nextToken(), equal_tokenizer.nextToken());
				}
				queryPlan.setQueryAttribute(queryAttribute);
				break;
			}
			case "rp":{
				queryPlan.setRecallProbability(sub_clause.get(3));
				break;
			}
			}
		}
		return queryPlan;	
	}
	
	
	public static QueryPlan getQueryPlan(){
		String SQLLine = readSQLLine();
		return parseSQLLine(SQLLine);
	}

}
