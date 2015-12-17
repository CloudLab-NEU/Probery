package com.neu.swc.tables;

import java.io.BufferedReader;
import java.io.IOException;

public class TableModel {

	public static Table getTable(BufferedReader reader) throws IOException{
		System.out.println("Create Table Model");
		Table table = new Table();
		ReferAttribute referAttribute;
		System.out.println("Table Name: ");
		table.setTableName(reader.readLine());
		
		System.out.println("ReferAttribute Name: ");
		String string = reader.readLine();
		while(!string.equals("")){
			referAttribute = new ReferAttribute();
			
			referAttribute.setName(string);
			System.out.println("ReferAttribute Type: ");
			referAttribute.setType(reader.readLine());
			System.out.println("ReferAttribute MinValue: ");
			referAttribute.setMinValue(reader.readLine());
			System.out.println("ReferAttribute MaxValue: ");
			referAttribute.setMaxValue(reader.readLine());
			System.out.println("ReferAttribute Size: ");
			referAttribute.setSize(reader.readLine());
			table.addReferAttribute(referAttribute);
			
			System.out.println("ReferAttribute Name: ");
			string = reader.readLine();
		}
		return table;
	}
}
