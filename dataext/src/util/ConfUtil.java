package util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

import org.apache.log4j.Logger;


/** 配置工具类
 * 
 * @pdOid a4f69055-951e-49b2-8004-38de8d055d88 */
public class ConfUtil {
   /** @pdOid d15c2bc7-d5de-47f9-abcc-779d429e8832 */
   private String confFile;
   /** @pdOid 9c435491-50b2-4b9b-937b-df1a7d9af624 */
   private Map<String,String> keyMap;
   
   static Logger logger = Logger.getLogger(ConfUtil.class.getName());
	
	public static String CONFFILE;
	private Map<String, String> map;
	private Map<String, String> tabMap;

	


	private static ConfUtil configUtil = null;
	
	private ConfUtil(String filename) {
		Properties properties = new Properties();
		map = new HashMap<String, String>();
		InputStream inputStream;
		try {
			inputStream = new FileInputStream(filename);
			properties.load(inputStream);
			Enumeration<Object> e = properties.keys();
			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();
				String val= (String) properties.getProperty(key);
				map.put(key, val);
				logger.debug(key+"="+val);
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	

	public synchronized static ConfUtil getInstance() {
		if (configUtil == null) {
			 configUtil = new ConfUtil(CONFFILE);
		} 
		return configUtil;
	}
	
	public String getVal(String key){
		return map.get(key);
	}
	
	
	public void put(String key,String val){
		 map.put(key, val);
	}



	public Map<String, String> getTabMap() {
		return tabMap;
	}



	public void setTabMap(Map<String, String> tabMap) {
		this.tabMap = tabMap;
	}
	
	
   /** @pdOid 97b34c1b-cadd-4e49-a590-6035d0c5a660 */
//   public ConfUtil(String conffile) {
//      // TODO: implement
////	   Properties prod  = 
//	   keyMap = new HashMap<String,String>();
//	   Properties properties = new Properties();
//		InputStream inputStream;
//		try {
//			inputStream = new FileInputStream(conffile);
//			properties.load(inputStream);  
//	        inputStream.close(); 
//	        
//	        keyMap.put("ISALL",properties.getProperty("ISALL"));
//	        keyMap.put("SCHEMAS",properties.getProperty("SCHEMAS"));
//	        keyMap.put("TABLES",properties.getProperty("TABLES"));
//	        keyMap.put("SOURCEDB",properties.getProperty("SOURCEDB"));
//	        keyMap.put("SOURCEOSUSER",properties.getProperty("SOURCEOSUSER"));
//	        keyMap.put("SOURCEOSPASS",properties.getProperty("SOURCEOSPASS"));
//	        keyMap.put("SOURCEDBUSER",properties.getProperty("SOURCEDBUSER"));
//	        keyMap.put("SOURCEDBPASS",properties.getProperty("SOURCEDBPASS"));
//	        keyMap.put("SOURCEOGGUSER",properties.getProperty("SOURCEOGGUSER"));
//	        keyMap.put("SOURCEOGGPASS",properties.getProperty("SOURCEOGGPASS"));
//	        keyMap.put("TARGETDB",properties.getProperty("TARGETDB"));
//	        keyMap.put("TARGETOSUSER",properties.getProperty("TARGETOSUSER"));
//	        keyMap.put("TARGETOSPASS",properties.getProperty("TARGETOSPASS"));
//	        keyMap.put("TARGETDBUSER",properties.getProperty("TARGETDBUSER"));
//	        keyMap.put("TARGETDBPASS",properties.getProperty("TARGETDBPASS"));
//	        keyMap.put("TARGETOGGUSER",properties.getProperty("TARGETOGGUSER"));
//	        keyMap.put("TARGETOGGPASS",properties.getProperty("TARGETOGGPASS"));
//	        keyMap.put("SOURCEOGGHOME",properties.getProperty("SOURCEOGGHOME"));
//	        keyMap.put("TARGETOGGHOME",properties.getProperty("TARGETOGGHOME"));
//	        
//	        
//	        
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}  
//   }
   
   /** @pdOid 448a147d-678b-4bb2-a989-e63c3c0cf441 */
//   public readConf() {
//      // TODO: implement
//   }
   
//   /** @pdOid 87457ba0-14ed-4b7b-a836-9a0167a041ed */
//   public String getValue() {
//      // TODO: implement
//      return null;
//   }

}
