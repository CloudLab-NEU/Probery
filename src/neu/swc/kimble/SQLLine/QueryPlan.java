package neu.swc.kimble.SQLLine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;

import neu.swc.kimble.ETL.KVPair;

public class QueryPlan {
	
	private ArrayList<String> select_key;
	private KVPair<String,String> queryAttribute, aggregation;
	private String table;
	private String recallProbability;
	
	public QueryPlan(){
		this.select_key = new ArrayList<String>();
		this.queryAttribute = new KVPair<String,String>();
		this.aggregation = new KVPair<String,String>();
	}
	
	
	public ArrayList<String> getSelect_key() {
		return select_key;
	}
	

	public void setSelect_key(ArrayList<String> select_key) {
		this.select_key = select_key;
	}

	public KVPair<String,String> getAggregation() {
		return this.aggregation;
	}

	public void setAggregation(KVPair<String,String> aggregation) {
		this.aggregation = aggregation;
	}

	public String getTableName() {
		return table;
	}


	public void setTable(String table) {
		this.table = table;
	}


	public KVPair<String,String> getQueryAttribute() {
		return queryAttribute;
	}
	
	public TreeMap<String,String> getQueryAttributeMap(){
		TreeMap<String,String> treeMap = new TreeMap<String,String>();
		Iterator<String> iterator = this.queryAttribute.iterator();
		while(iterator.hasNext())
			treeMap.put(iterator.next(), this.queryAttribute.getCorrespondingValue());
		return treeMap;
	}


	public void setQueryAttribute(KVPair<String,String> queryAttribute) {
		this.queryAttribute = queryAttribute;
	}


	public Double getRecallProbability() {
		return Double.parseDouble(recallProbability);
	}


	public void setRecallProbability(String recallProbability) {
		this.recallProbability = recallProbability;
	}
	 
}
