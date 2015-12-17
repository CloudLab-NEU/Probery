package com.neu.swc.index;

import java.io.IOException;
import java.util.ArrayList;

import com.neu.swc.SQLLine.QueryPlan;
import com.neu.swc.etl.KVPair;
import com.neu.swc.tables.ReferAttribute;
import com.neu.swc.tables.Table;
import com.neu.swc.tables.TableList;

public class Index {

	private TableList tableList;
	
	public Index(TableList tableList) throws IOException{
		this.tableList = tableList;
	}
	
	public void refresh(TableList tableList){
		this.tableList = tableList;
	}
	
	public int getBlockNumber(QueryPlan queryPlan){
		ReferAttribute referAttribute;
		Table table = this.tableList.getTable(queryPlan.getTableName());
		ArrayList<ReferAttribute> referAttributeList = table.getReferAttributeList();
		KVPair<String,String> queryAttribute = queryPlan.getQueryAttribute();
		int[] attriRegionSize = new int[referAttributeList.size()];
		int[] coordinate = new int[referAttributeList.size()];
		for(int i = 0; i < referAttributeList.size(); i++){
			referAttribute = referAttributeList.get(i);
			attriRegionSize[i] = referAttribute.getPartitionSize();
			coordinate[i] = referAttribute.getAttriRegionNumber(queryAttribute.get(referAttribute.getName()));
		}
		
		int variable1 = 1;
		int variable2 = 0;
		
		for(int i = referAttributeList.size()-1; i >= 0; i--)
		{
			for(int j = 0; j < i; j++)
			{
				variable1 = variable1 * attriRegionSize[j];
			}
			if(i == 0)
				variable2 = variable2 + coordinate[i];
			else
			{
				variable2 = variable2 + (coordinate[i]-1) * variable1;
				variable1 = 1;
			}
			
		}
		return variable2;
	}
}
