package com.neu.swc.tables;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.SAXReader;
import org.dom4j.io.XMLWriter;


public class XMLTable {
	
	private final static String url = "/home/kimble/Probery/MetaData/tables.xml";
	private final static Logger logger = Logger.getLogger(XMLTable.class);
	
	public XMLTable(){
		
	}
	
	@SuppressWarnings("unchecked")
	public static ArrayList<Table> getTableList(){
		ArrayList<Table> tables = new ArrayList<Table>();
 		Table table;
		ReferAttribute referAttribute;
		ArrayList<ReferAttribute> referAttributeList;
		
		SAXReader reader = new SAXReader();
		Document document;
		try {
			document = reader.read(new File(url));
		} catch (DocumentException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
			logger.warn("There is not Tables in DataBase");
			XMLTable.createTableDocument();
			return new ArrayList<Table>();
		}
		
		Element root = document.getRootElement();
		List<Element> noodTable = root.elements();
		List<Element> noodReferAttribute, noodAttributeDescribe;
		Element tableElement, referAttributeElement,referAttributeDescribeElement;
		for(Iterator<Element> iteratorTable = noodTable.iterator(); iteratorTable.hasNext();){
			table = new Table();
			referAttributeList = new ArrayList<ReferAttribute>();
			tableElement = iteratorTable.next();
			table.setTableName(tableElement.getName());
			noodReferAttribute = tableElement.elements();
			for(Iterator<Element> iteratorReferAttribute = noodReferAttribute.iterator();iteratorReferAttribute.hasNext();){
				referAttribute = new ReferAttribute();
				referAttributeElement = iteratorReferAttribute.next();
				referAttribute.setName(referAttributeElement.getName());
				noodAttributeDescribe = referAttributeElement.elements();
				for(Iterator<Element> iteratorReferAttributeDescribe = noodAttributeDescribe.iterator();iteratorReferAttributeDescribe.hasNext();){
					referAttributeDescribeElement = iteratorReferAttributeDescribe.next();
					switch(referAttributeDescribeElement.getName())
					{
					case "type":
						referAttribute.setType(referAttributeDescribeElement.getData().toString());
						break;
					case "min_value":
						referAttribute.setMinValue(referAttributeDescribeElement.getData().toString());
						break;
					case "max_value":
						referAttribute.setMaxValue(referAttributeDescribeElement.getData().toString());
						break;
					case "size":
						referAttribute.setSize(referAttributeDescribeElement.getData().toString());
						break;
					}
				}
				referAttributeList.add(referAttribute);
			}
			table.setReferAttributeList(referAttributeList);
			tables.add(table);
			
		}
		return tables;
	}
	
	private static void createTableDocument(){  
		Document document = DocumentHelper.createDocument();
		document.addElement("Tables");
		OutputFormat format = OutputFormat.createPrettyPrint();
		XMLWriter output;
		try {
			output = new XMLWriter(new FileWriter(url),format);
			output.write(document);
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
		}
	}
	
	public static void addTable(Table table) throws IOException, DocumentException{
		SAXReader reader = new SAXReader();
		Document document = reader.read(new File(url));
		Element root = document.getRootElement();
		Element nood = root.addElement(table.getTableName());
		Element sub_nood;
		for(ReferAttribute referAttribute:table.getReferAttributeList()){
			sub_nood = nood.addElement(referAttribute.getName());
			sub_nood.addElement("type").addText(referAttribute.getType());
			sub_nood.addElement("min_value").addText(referAttribute.getMinValue());
			sub_nood.addElement("max_value").addText(referAttribute.getMaxValue());
			sub_nood.addElement("size").addText(referAttribute.getSize());
		}
		saveDocument(document);
	}
	
	private static void saveDocument(Document document) throws IOException{
		OutputFormat format = OutputFormat.createPrettyPrint();
		XMLWriter output = new XMLWriter(new FileWriter(url),format);
		output.write(document);
		output.close();
	}
	
	public static boolean deleteTable(Table table){
		return true;
	}
}
