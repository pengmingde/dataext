package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;






import java.util.Random;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import bean.Column;
import bean.Table;
import config.Config;

public class ValidateUtil {
	static Logger logger = Logger.getLogger(ValidateUtil.class.getName());
	public static void validate(ConfUtil cu){
//		1、检查所有必填参数是否配置
		if(!checkParams(cu)){
			logger.error("Fail:params not configure correct,exit!");
			System.exit(-1);
		}else{
			logger.info("check params passed!");
		}
//		2、检查能否登录源库、目标库，并组装好ConfUtil的4个链接（源库数据库用户、源库OGG用户，目标库数据库用户、目标库OGG用户）
		if(!checkLogin(cu)){
			logger.error("Fail:can't login to database,exit!");
			System.exit(-1);
		}else{
			logger.info("check dblogin passed!");
		}
//		3、检查TABLES参数的配置格式是否正确，如果正确则组装好ConfUtil的tabMap，
//		tabMap的保存内容为TABLE1：column1，column2 TABLE2:ALLCOLUMN
		if(!checkTabColFormat(cu)){
//			logger.error("Fail:table or column not exist,exit!");
			System.exit(-1);
		}else{
			logger.info("check table&column format passed!");
		}
		
		
//		4、获取source端需要extract的所有SCHEMA，并返回
		List<String> schemas=getSchemsByCu(cu);
		
		if(schemas.size()==0){
			logger.error("schema can't been empty,exit!");
			System.exit(-1);
		}else{
			logger.info("check schema available passed!");
		}
//		组装ConfUtil的schemas
		cu.setSchemas(schemas);
		
//		5、检查表和列在源端是否存在
		if(!checkTabColExist(cu)){
			logger.error("Fail:table or column not exist,exit!");
			System.exit(-1);
		}else{
			logger.info("check table&column available passed!");
		}
//		6、根据CU生成表列对象
		List<Table> tables= getTablesByCu(cu);
		if(tables.size()==0){
			logger.error("getTablesByCu return empty empty,exit!");
			System.exit(-1);
		}else{
			cu.setTables(tables);
			logger.debug(tables);
			logger.info("getTablesByCu passed!");
		}

//		7、检查Target端是否有对应表，如果没有的话，需要创建
		checkTabColExistInTarget(cu);
		
	}
	
	private static List<Table> getTablesByCu(ConfUtil cu){
		Map<String, String> tabMap = cu.getTabMap();
		String sql="";
		List<Table> tables= new ArrayList<Table> ();
		String sch = cu.getSchemas().get(0);
		
		Statement stat=null;
		ResultSet rs=null;
		if(tabMap.size()==0){
			logger.error("tabMap is empty,exit!");
			System.exit(-1);
		}
		
		Iterator<Entry<String, String>> iter = tabMap.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, String> entry = iter.next(); 
			String tabname = entry.getKey();
			String columns = entry.getValue(); 
			
			
			Table tabBean = new Table();
			tabBean.setTable_name(tabname);
			
			tabBean.setColumns(new TreeMap<Integer, Column>());
			if(Config.ALLCOLUMN.equals(columns)){
				sql="select column_id,column_name,data_type,owner, listagg(column_name,',') within group(order by column_id) over(partition by null) columnstr from dba_tab_cols where owner='"+sch+"' and table_name='"+tabname+"' order by column_id ";
			}else{
				
				sql="select column_id,column_name,data_type,owner,listagg(column_name,',') within group(order by column_id) over(partition by null) columnstr from dba_tab_cols where owner='"+sch+"' and table_name ='"+tabname+"' and column_name in ("+StringUtil.strToInlist(columns)+") order by column_id ";
			}
			
			try {
				stat=cu.getConnSourceDB().createStatement();
				logger.debug(sql);
				rs=stat.executeQuery(sql);
				while(rs.next()){
//					tabcnt=rs.getInt("tabcnt");
					Column colBean = new Column();
					colBean.setColumn_id(rs.getInt("column_id"));
					colBean.setColumn_name(rs.getString("column_name"));
					colBean.setData_type(rs.getString("data_type"));
					colBean.setOwner(rs.getString("owner"));
					tabBean.getColumns().put(colBean.getColumn_id(), colBean);
					tabBean.setColumnstr(rs.getString("columnstr"));
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			tables.add(tabBean);
//				String sql="select "+columns+" from "+sch+"."+tabname+" where rownum=1";
//				logger.debug("verify sql:"+sql);
//				stat=cu.getConnTargetDB().createStatement();
//				stat.execute(sql);
		}
		
		return tables;
	}
	
	private static List<String> getSchemsByCu(ConfUtil cu){
		Statement stat=null;
		ResultSet rs=null;
		int entcnt=0;
		List<String> schemas = new ArrayList<String>();
		String sql;
		try {
			stat=cu.getConnSourceDB().createStatement();
			
			if("Y".equals(cu.getVal("ISALL"))){
				sql="select 'ZT_'||entcode entcode from weipos.p_entinfo where isprodent=1";
				logger.debug(sql);
				rs=stat.executeQuery(sql);
				while(rs.next()){
					String schema=rs.getString("entcode");
					schemas.add(schema);
				}
				return schemas;
			}else if("N".equals(cu.getVal("ISALL"))&&cu.getVal("SCHEMAS")!=null){
				String schemaArray[] = cu.getVal("SCHEMAS").toUpperCase().split(",");
//				String schsContByComma="";
//				for(String sch:schemaArray){
//					schsContByComma+=",'"+sch.toUpperCase().replace("ZT_", "")+"'";
//				}
//				schsContByComma=schsContByComma.substring(1);
				sql="select count(*) entcnt from weipos.p_entinfo where isprodent=1 and entcode in ("+StringUtil.strToInlist(cu.getVal("SCHEMAS"))+")";
				logger.debug(sql);
				rs=stat.executeQuery(sql);
				if(rs.next()){
					entcnt=rs.getInt("entcnt");
				}
				if(entcnt!=schemaArray.length){
					logger.error("parameter SCHEMAS wrong,some schemas not exist in weipos.entinfo,exit!");
					System.exit(-1);
				}
				schemas.addAll(Arrays.asList(schemaArray));
			}
			
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e1));
			return schemas;
		}finally{
			ConnUtil.freeRs(rs);
			ConnUtil.freeStat(stat);
		}
		
		return schemas;
	}
	
//	检查目标schema下是否有同名的表：
//	1）如果没有则创建表
//	2）如果有则检查列是否一致，如果列不一致，则备份走，并重建一个符合要求的表
	private static void checkTabColExistInTarget(ConfUtil cu) {
		Statement stat=null;
		ResultSet rs=null;
		int tabcnt=0;
		String sql="";
		
		try {
			List<Table> tables = cu.getTables();
//			Map<String,String> tabMap = cu.getTabMap();
			if(tables.size()==0){
				logger.error("tables is empty,exit!");
				System.exit(-1);
			}
			
			for(Table tab :tables){
				String tabname = tab.getTable_name();
				String columns = tab.getColumnstr(); 
				logger.debug("tabname="+tabname+",columns="+columns);
				sql="select count(*) tabcnt from user_tables where table_name='"+tabname+"'";
				stat=cu.getConnTargetDB().createStatement();
				rs=stat.executeQuery(sql);
				if(rs.next()){
					tabcnt=rs.getInt("tabcnt");
				}
				if(tabcnt==0){
					logger.info("table "+tabname+" not exist on target,create it now！ ");
//					sql="select column_name from dba_tab_cols where table_name=''";
					createTabOnTarget(cu,tab);
				}else{
//					检查表、列是否匹配，如果匹配则OK，如果不匹配，则备份并重建表
//					检查表、列是否匹配
					if(checkTabColMap(cu,tab)){
						logger.info("The table "+tab.getTable_name()+ " is exist on target and the table can be use!");
						deleteTargetDataBySchema(cu,tab);
					}else{
						logger.info("The table "+tab.getTable_name()+ " is exist on target but the table can't be use.");
						if(backupAndDropTabOnTarget(cu,tab)){
							logger.info("Backup and drop table "+tab.getTable_name()+ " success.");
						}else{
							logger.info("Backup and drop table "+tab.getTable_name()+ " fail.");
						}
						if(createTabOnTarget(cu,tab)){
							logger.info("create table "+tab.getTable_name()+ " success.");
						}else{
							logger.info("create table "+tab.getTable_name()+ " fail.");
						}
					}
//					checkColForTab(cu,tabname);
				}
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
		}finally{
			ConnUtil.freeRs(rs);
			ConnUtil.freeStat(stat);
		}
	}
	
	private static boolean deleteTargetDataBySchema(ConfUtil cu,Table tab){
		boolean succ=true;
		Statement st=null;
		String sql="";
		int delCount=0;
		try {
			st=cu.getConnTargetDB().createStatement();
			for(String schema:cu.getSchemas()){
				sql="delete "+tab.getTable_name()+" where encode in ('"+schema+"')";
				delCount=st.executeUpdate(sql);
				logger.info("delete "+schema+"'s data on "+tab.getTable_name()+","+delCount+" rows.");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
			succ=false;
		}finally{
			ConnUtil.freeStat(st);
		}
		
		return succ;
	}
//	在TARGET端备份表
	private static boolean backupAndDropTabOnTarget(ConfUtil cu,Table tab){
		boolean succ=true;
		Statement st=null;
		String newtabname=tab.getTable_name()+new Random().nextInt(3);
		String sqlcreate="create table "+newtabname+" as select * from "+tab.getTable_name();
		String sqldrop="drop table "+tab.getTable_name();
		try {
			st=cu.getConnTargetDB().createStatement();
			logger.debug(sqlcreate);
			st.execute(sqlcreate);
			logger.debug(sqldrop);
			st.execute(sqldrop);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			succ=false;
			logger.error(LogUtil.getStackTrace(e));
		}finally{
			ConnUtil.freeStat(st);
		}
		return succ;
	}
//	检查表、列是否匹配,目前只检查列是否都存在于目标表的列
	private static boolean checkTabColMap(ConfUtil cu,Table tab){
		boolean isMap=true;
		Statement st=null;
		ResultSet rs=null;
		List<String> cols = new ArrayList<String>();
		cols.add("ENCODE");
		String sql="select column_name from dba_tab_cols where owner='"+cu.getVal("TARGETDBUSER")+"' and table_name='"+tab.getTable_name()+"' order by column_id";
		try {
			st=cu.getConnTargetDB().createStatement();
			rs=st.executeQuery(sql);
			while(rs.next()){
				cols.add(rs.getString("column_name"));
			}
			
			Iterator<Entry<Integer, Column>> iter = tab.getColumns().entrySet().iterator();
			while(iter.hasNext()){
				Entry<Integer, Column> entry = iter.next(); 
				Column column = entry.getValue();
				if(!cols.contains(column.getColumn_name())){
					logger.debug("column not maped:"+column.getColumn_name());
					isMap=false;
					break;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
		}finally{
			ConnUtil.freeRs(rs);
			ConnUtil.freeStat(st);
		}
		return isMap;
	}
	
	private static boolean createTabOnTarget(ConfUtil cu,Table tab){
		boolean succ=true;
		Statement stat=null;
		String sql="create table "+tab.getTable_name()+"(encode VARCHAR2(30),";
		String sqlcols="";
		Iterator<Entry<Integer, Column>> iter = tab.getColumns().entrySet().iterator();
		while(iter.hasNext()){
			Entry<Integer, Column> entry = iter.next(); 
			Column column = entry.getValue(); 
			sqlcols+=","+column.getColumn_name()+" "+column.getData_type2();
		}
		sql+=sqlcols.substring(1);
				sql+=")";
		try {
			stat=cu.getConnTargetDB().createStatement();
			stat.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
			succ=false;
		}finally{
			ConnUtil.freeStat(stat);
		}
		return succ;
	}
	
	private static boolean checkTabColExist(ConfUtil cu) {
		Statement stat=null;
		
		try {
			
			Map<String,String> tabMap = cu.getTabMap();
			if(tabMap.size()==0){
				logger.error("tabMap is null!");
				return false;
			}
			Iterator<Entry<String, String>> iter = tabMap.entrySet().iterator();
			while(iter.hasNext()){
				Entry<String, String> entry = iter.next(); 
				String tabname = entry.getKey();
				String columns = entry.getValue(); 
				logger.debug("tabname="+tabname+",columns="+columns);
				for(String sch:cu.getSchemas()){
					String sql="select "+columns+" from "+sch+"."+tabname+" where rownum=1";
					logger.debug("verify sql:"+sql);
					stat=cu.getConnSourceDB().createStatement();
					stat.execute(sql);
				}
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}finally{
			ConnUtil.freeStat(stat);
		}
		
		return true;
	}
	
	private static boolean checkLogin(ConfUtil cu) {
	      // TODO: implement
//		"jdbc:oracle:thin:@"+schemaBeanNew.getIPADDR()+":"+schemaBeanNew.getPORTADDR()+":"+schemaBeanNew.getDBSID()
		String url="jdbc:oracle:thin:@"+cu.getVal("SOURCEDBIP")+":"+cu.getVal("SOURCEDBPORT")+":"+cu.getVal("SOURCEDBSID");
		cu.put("SOURCEDBURL", url);
		try {
			Connection connSourceDB=DriverManager.getConnection(cu.getVal("SOURCEDBURL"),cu.getVal("SOURCEDBUSER"), cu.getVal("SOURCEDBPASS"));
			cu.setConnSourceDB(connSourceDB);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("SOURCEDBURL")+",username="+cu.getVal("SOURCEDBUSER")+",password="+cu.getVal("SOURCEDBPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		try {
			Connection connSourceOGG = DriverManager.getConnection(cu.getVal("SOURCEDBURL"),cu.getVal("SOURCEOGGUSER"), cu.getVal("SOURCEOGGPASS"));
			cu.setConnSourceOGG(connSourceOGG);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("SOURCEDBURL")+",username="+cu.getVal("SOURCEOGGUSER")+",password="+cu.getVal("SOURCEOGGPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		
		url="jdbc:oracle:thin:@"+cu.getVal("TARGETDBIP")+":"+cu.getVal("TARGETDBPORT")+":"+cu.getVal("TARGETDBSID");
		cu.put("TARGETDBURL", url);
		try {
			Connection connTargetDB=DriverManager.getConnection(cu.getVal("TARGETDBURL"),cu.getVal("TARGETDBUSER"), cu.getVal("TARGETDBPASS"));
			cu.setConnTargetDB(connTargetDB);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("TARGETDBURL")+",username="+cu.getVal("TARGETDBUSER")+",password="+cu.getVal("TARGETDBPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		try {
			Connection connTargetOGG =  DriverManager.getConnection(cu.getVal("TARGETDBURL"),cu.getVal("TARGETOGGUSER"), cu.getVal("TARGETOGGPASS"));
			cu.setConnTargetOGG(connTargetOGG);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("TARGETDBURL")+",username="+cu.getVal("TARGETOGGUSER")+",password="+cu.getVal("TARGETOGGPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
	      return true;
	   }
	   
	   /** @pdOid ac8d68d7-ddcf-4a98-9336-a3c99486b59e */
	   private static boolean checkTabColFormat(ConfUtil cu) {
	      // TODO: implement
		   Map<String,String> tabMap = new HashMap<String,String>();
		   String tabAndCol[]=cu.getVal("TABLES").split(";");
		   for(String tbCol:tabAndCol){
			   String tbOrCol[] = tbCol.split(":");
			   if(tbOrCol.length>2){
				   logger.error("Table format is wrong："+tbCol);
				   return false;
//				   tabMap.put(tbOrCol[0], "");
			   }
			   tabMap.put(tbOrCol[0], (tbOrCol.length==1)?Config.ALLCOLUMN:tbOrCol[1]);
		   }
		   cu.setTabMap(tabMap);
	      return true;
	   }
	   
	   private static boolean checkParamsConflict(ConfUtil cu) {
		   String isall=cu.getVal("ISALL");
			String schemas=cu.getVal("SCHEMAS");
			if(isall==null&&schemas==null){
				logger.error("Neither ISALL Nor SCHEMAS be configured!");
//				System.exit(-1);
				 return false;
			}
			if(isall!=null&&schemas!=null&&isall.equals("Y")){
				logger.error("When SCHEMAS be configured,ISALL can't be Y!");
//				System.exit(-1);
				 return false;
			}
			return true;
	   }
	   /** @pdOid f4d1c7d3-63af-4c25-985c-9b0a7b7f07d8 */
	   private static boolean checkParams(ConfUtil cu) {
//			检查必填参数
			
			if(!checkParamsMust(cu,"SOURCEDBIP"))  return false;
			if(!checkParamsMust(cu,"SOURCEDBSID"))  return false;
			if(!checkParamsMust(cu,"SOURCEDBPORT"))  return false;
			if(!checkParamsMust(cu,"SOURCEOSUSER"))  return false;
			if(!checkParamsMust(cu,"SOURCEOSPASS"))  return false;
			if(!checkParamsMust(cu,"SOURCEDBUSER"))  return false;
			if(!checkParamsMust(cu,"SOURCEDBPASS"))  return false;
			if(!checkParamsMust(cu,"SOURCEOGGUSER"))  return false;
			if(!checkParamsMust(cu,"SOURCEOGGPASS"))  return false;
					
			if(!checkParamsMust(cu,"TARGETDBIP"))  return false;
			if(!checkParamsMust(cu,"TARGETDBSID"))  return false;
			if(!checkParamsMust(cu,"TARGETDBPORT"))  return false;
			if(!checkParamsMust(cu,"TARGETOSUSER"))  return false;
			if(!checkParamsMust(cu,"TARGETOSPASS"))  return false;
			if(!checkParamsMust(cu,"TARGETDBUSER"))  return false;
			if(!checkParamsMust(cu,"TARGETDBPASS"))  return false;
			if(!checkParamsMust(cu,"TARGETOGGUSER"))  return false;
			if(!checkParamsMust(cu,"TARGETOGGPASS"))  return false;
			
			if(!checkParamsMust(cu,"TABLES"))  return false;
			if(!checkParamsMust(cu,"SOURCEOGGHOME"))  return false;
			if(!checkParamsMust(cu,"TARGETOGGHOME"))  return false;
			
//			if(!checkParamsDBValueFormat(cu,"SOURCEDB"))  return false;
//			if(!checkParamsDBValueFormat(cu,"TARGETDB"))  return false;
	      return true;
	   }
	   
	   /** @pdOid f4d1c7d3-63af-4c25-985c-9b0a7b7f07d8 */
	   private static boolean checkParamsMust(ConfUtil cu,String key) {
		   	if(cu.getVal(key)==null||"".equals(cu.getVal(key))){
				logger.error("Fail:param "+key+" is need!");
//				System.exit(-1);
				 return false;
			}
	      return true;
	   }
	   
//	   private static boolean checkParamsDBValueFormat(ConfUtil cu,String key) {
//		   String val = cu.getVal(key);
//		   if(val.indexOf(":")<0||val.split(":").length!=3){
//			   logger.error("Fail:param "+key+" format wrong,the right format is: IP:sid:port");
//			   return false;
//		   }
//
//	      return true;
//	   }
}

