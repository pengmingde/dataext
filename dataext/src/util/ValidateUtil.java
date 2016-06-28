package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import main.Dataext;

import org.apache.log4j.Logger;

import config.Config;

public class ValidateUtil {
	static Logger logger = Logger.getLogger(ValidateUtil.class.getName());
	public static void validate(ConfUtil cu){
		if(!checkParams(cu)){
			logger.error("Fail:params not configure correct,exit!");
			System.exit(-1);
		}
		
		if(!checkLogin(cu)){
			logger.error("Fail:can't login to database,exit!");
			System.exit(-1);
		}
		
		if(!checkTabColFormat(cu)){
//			logger.error("Fail:table or column not exist,exit!");
			System.exit(-1);
		}
		
		if(!checkTabColExist(cu)){
			logger.error("Fail:table or column not exist,exit!");
			System.exit(-1);
		}
	}
	
	private static boolean checkTabColExist(ConfUtil cu) {
		Connection conn=null;
		Statement stat=null;
		try {
			conn = DriverManager.getConnection(cu.getVal("SOURCEDBURL"),cu.getVal("SOURCEOGGUSER"), cu.getVal("SOURCEOGGPASS"));
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e1));
			return false;
		}
		
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
				String sql="select "+columns+" from "+tabname+" where rownum=1";
				logger.debug("verify sql:"+sql);
				stat=conn.createStatement();
				stat.execute(sql);
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}finally{
			
				try {
					if(stat!=null)
						stat.close();
					if(conn!=null)
						conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					logger.error(LogUtil.getStackTrace(e));
				}
			
		}
		
		return true;
	}
	
	private static boolean checkLogin(ConfUtil cu) {
	      // TODO: implement
//		"jdbc:oracle:thin:@"+schemaBeanNew.getIPADDR()+":"+schemaBeanNew.getPORTADDR()+":"+schemaBeanNew.getDBSID()
		String url="jdbc:oracle:thin:@"+cu.getVal("SOURCEDBIP")+":"+cu.getVal("SOURCEDBPORT")+":"+cu.getVal("SOURCEDBSID");
		cu.put("SOURCEDBURL", url);
		try {
			DriverManager.getConnection(cu.getVal("SOURCEDBURL"),cu.getVal("SOURCEDBUSER"), cu.getVal("SOURCEDBPASS"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			
			logger.error("Login fail<url="+cu.getVal("SOURCEDBURL")+",username="+cu.getVal("SOURCEDBUSER")+",password="+cu.getVal("SOURCEDBPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		try {
			DriverManager.getConnection(cu.getVal("SOURCEDBURL"),cu.getVal("SOURCEOGGUSER"), cu.getVal("SOURCEOGGPASS"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("SOURCEDBURL")+",username="+cu.getVal("SOURCEOGGUSER")+",password="+cu.getVal("SOURCEOGGPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		
		url="jdbc:oracle:thin:@"+cu.getVal("TARGETDBIP")+":"+cu.getVal("TARGETDBPORT")+":"+cu.getVal("TARGETDBSID");
		cu.put("TARGETDBURL", url);
		try {
			DriverManager.getConnection(cu.getVal("TARGETDBURL"),cu.getVal("TARGETDBUSER"), cu.getVal("TARGETDBPASS"));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Login fail<url="+cu.getVal("TARGETDBURL")+",username="+cu.getVal("TARGETDBUSER")+",password="+cu.getVal("TARGETDBPASS")+">");
			logger.error(LogUtil.getStackTrace(e));
			return false;
		}
		
		try {
			DriverManager.getConnection(cu.getVal("TARGETDBURL"),cu.getVal("TARGETOGGUSER"), cu.getVal("TARGETOGGPASS"));
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

