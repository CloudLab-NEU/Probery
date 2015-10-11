package neu.swc.kimble.tables;
import java.io.IOException;
import java.util.ArrayList;

import org.dom4j.DocumentException;

public class TableList {

	private ArrayList<Table> tableList;
	
	public TableList(){
		this.tableList = XMLTable.getTableList();
	}
	
	public ArrayList<Table> getTable(){
		return this.tableList;
	}
	public void refresh() throws DocumentException{
		this.tableList = XMLTable.getTableList();
	}
	
	public void addTable(Table table) throws IOException, DocumentException{
		this.tableList.add(table);
		XMLTable.addTable(table);
	}
	
	public void deleteTable(Table table){
		this.tableList.remove(table);
		XMLTable.deleteTable(table);
	}
	
	public Table getTable(String tableName){
		for(Table table:this.tableList){
			if(table.getTableName().equals(tableName))
				return table;
		}
		return null;
	}
	
	public boolean isNull(){
		if(this.tableList.size() == 0)
			return true;
		else
			return false;
	}
}
