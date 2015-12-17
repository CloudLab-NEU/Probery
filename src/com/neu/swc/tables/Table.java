package com.neu.swc.tables;
import java.util.ArrayList;


public class Table {

	private String tableName;
	private ArrayList<ReferAttribute> referAttributeList;
	
	
	public Table(){
		this.referAttributeList = new ArrayList<ReferAttribute>();
	}
	
	public Table(String tableName, ArrayList<ReferAttribute> referAttributeList){
		this.tableName = tableName;
		this.referAttributeList = referAttributeList;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ArrayList<ReferAttribute> getReferAttributeList() {
		return referAttributeList;
	}

	public void setReferAttributeList(ArrayList<ReferAttribute> referAttributeList) {
		this.referAttributeList = referAttributeList;
	}
	
	public void addReferAttribute(ReferAttribute referAttribute){
		this.referAttributeList.add(referAttribute);
	}
}
