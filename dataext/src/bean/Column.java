package bean;

public class Column {
private String column_name;
private String data_type;
private int column_id;
private String owner;
//private int data_length;
//private String data_type2;


public String getData_type2() {
//	switch (data_type){
//	case "VARCHAR2": data_type2="VARCHAR2(4000)" ;break;
//	case "NUMBER": data_type2="(4000)" ;break;
//	}
	if(data_type!=null&&data_type.equals("VARCHAR2"))
		return " VARCHAR2(4000) ";
	return data_type;
}

public String getColumn_name() {
	return column_name;
}
public void setColumn_name(String column_name) {
	this.column_name = column_name;
}
public String getData_type() {
	return data_type;
}
public void setData_type(String data_type) {
	this.data_type = data_type;
}
public int getColumn_id() {
	return column_id;
}
public void setColumn_id(int column_id) {
	this.column_id = column_id;
}

public String getOwner() {
	return owner;
}
public void setOwner(String owner) {
	this.owner = owner;
}
@Override
public String toString() {
	// TODO Auto-generated method stub
	return "<column_id:"+column_id+",column_name:"+column_name+",data_type:"+data_type+",owner:"+owner+">";
}


}
