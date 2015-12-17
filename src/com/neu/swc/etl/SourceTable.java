package com.neu.swc.etl;

public class SourceTable {

	private String[] attribute;
	private int[] referAttribute;
	
	public SourceTable(){
		
	}
	
	public SourceTable(String[] attribute, int[] referAttribute){
		this.attribute = attribute;
		this.referAttribute = referAttribute;
	}
	
	public String[] getAttribute(){
		return this.attribute;
	}
	
	public int getSize(){
		return this.attribute.length;
	}	
	
	public int[] getReferAttributeLocation(){
		return this.referAttribute;
	}
	
	public int[] getOtherAttributeLocation(){
		int[] otherAttributeLocaion = new int[this.attribute.length-this.referAttribute.length];
		int temp = 0;
		boolean flag;
		for(int i =0; i<this.attribute.length;i++){
			flag = true;
			for(int j = 0; j<this.referAttribute.length;j++){
				if(i == this.referAttribute[j]){
					flag = false;
					break;
				}
			}
			if(flag){
				otherAttributeLocaion[temp] = i;
				temp = temp + 1;
			}
		}
		return otherAttributeLocaion;
	}
	
	public int[] getAttributeMark(){
		int[] attributeMark = new int[this.attribute.length];
		for(int i=0; i<this.referAttribute.length; i++){
			for(int j=0; j<attributeMark.length; j++){
				if(j == this.referAttribute[i])
					attributeMark[j] = 1;
				else
					attributeMark[j] =0;
			}
		}
		return attributeMark;
	}
}
