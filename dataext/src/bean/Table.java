package bean;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.Map.Entry;

public class Table {

	private String table_name;
	private String owner;
	private SortedMap<Integer,Column> columns;
	private String columnstr;
	
	public String getColumnstr() {
		return columnstr;
	}
	public void setColumnstr(String columnstr) {
		this.columnstr = columnstr;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner = owner;
	}
	public SortedMap<Integer, Column> getColumns() {
		return columns;
	}
	public void setColumns(SortedMap<Integer, Column> columns) {
		this.columns = columns;
	}
	
	public String toString() {
		// TODO Auto-generated method stub
		String res="";
		Iterator<Entry<Integer, Column>> iter = columns.entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, Column> entry = iter.next(); 
			Integer column_id = entry.getKey();
			Column column = entry.getValue(); 
			res+=" [column_id:"+column_id+",column:"+column+"]";
		}
		return "table_name:"+table_name+","+res;
	}

}
