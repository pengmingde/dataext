package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.log4j.Logger;

import config.Config;
import bean.Table;

public class ParamFileUtil {
	static Logger logger = Logger.getLogger(ParamFileUtil.class.getName());
	private static String NEWLINE=System.getProperty("line.separator");
	
	private static String EXTHEAD="extract #EXTNAME#"+NEWLINE+
			"setenv (NLS_LANG=AMERICAN_AMERICA.AL32UTF8)"+NEWLINE+
			"setenv (ORACLE_SID=test)"+NEWLINE+
//			"--TRANLOGOPTIONS ASMUSER sys@asminst, asmpassword oracle"+
			"userid goldengate,password goldengate"+NEWLINE+
			"REPORTCOUNT EVERY 1 MINUTES, RATE"+NEWLINE+
			"numfiles 5000"+NEWLINE+
			"DISCARDFILE ./dirrpt/ext_bill.dsc,APPEND,MEGABYTES 1000"+NEWLINE+
			"DISCARDROLLOVER AT 3:00"+NEWLINE+
			"exttrail ./dirdat/b0,megabytes 100"+NEWLINE+
			"dynamicresolution"+NEWLINE+
			"tranlogoptions rawdeviceoffset 0"+NEWLINE+
			"TRANLOGOPTIONS EXCLUDEUSER goldengate"+NEWLINE+
			"TRANLOGOPTIONS convertucs2clobs"+NEWLINE+
			"TARGETDEFS ./dirdef/bill.def"+NEWLINE;
	
	private static String EXTMODEL="TABLE #SCHEMA#.#TABLE#"+NEWLINE+
			"token( encode = #SCHEMA#)"+NEWLINE+
			"COLS(#COLS#)"+NEWLINE+
			"KEYCOLS(#KEYCOLS#);"+NEWLINE;
	

	private static String SOURCEDEFPARHEAD="DEFSFILE ./"+Config.DIRDEF+"/#EXTNAME#"+Config.DEFSUFFIX+",purge"+NEWLINE+
			"USERID #USERNAME#,PASSWORD #PASSWORD#"+NEWLINE;
	private static String SOURCEDEFPARMODEL="TABLE #SCHEMA#.#TABLE#;"+NEWLINE;
//	生成source的def par文件内容
//	[oracle@jwdb dirprm orcl2]$cat ztbill.par
//	DEFSFILE ./dirdef/ztbill.def,purge
//	USERID ZT_110,PASSWORD ZT_110
//	TABLE ZT_110.BU_BILL;
//	USERID ZT_120,PASSWORD ZT_120
//	TABLE ZT_120.BU_BILL;
	public static String generateSourceDefParContent(ConfUtil cu,String extname){
		StringBuffer defParContent =new StringBuffer(SOURCEDEFPARHEAD.replaceAll("#EXTNAME#", extname)
				.replaceAll("#USERNAME#", cu.getVal("TARGETDBUSER")).replaceAll("#PASSWORD#", cu.getVal("TARGETDBPASS")));
		List<String> schemas= cu.getSchemas();
		List<Table> tables= cu.getTables();
		
		for(String schema:schemas){
			for(Table table:tables){
				defParContent.append(SOURCEDEFPARMODEL.replaceAll("#SCHEMA#", schema).replaceAll("#TABLE#", table.getTable_name()));
			}
		}
		return defParContent.toString();
	}
//	生成source的def par文件,返回文件名
	public static String generateSourceDefParFILE(ConfUtil cu,String extname,String content){
		String defParFile=cu.getVal("WORKDIR")+extname+Config.DEFPARFILESUFFIX;
		logger.debug("defParFile="+defParFile);
		File f = new File(defParFile);
		BufferedWriter fw =null;
//		if (!f.getParentFile().exists()) {
//			f.getParentFile().mkdirs();
//		}
		
		try {
			if (!f.exists()) {
				f.createNewFile();
			}
			fw =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "GBK")) ;
			fw.write(content);
			fw.flush();
			
		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return defParFile;
	}
	
//	TABLE ZT_110.BU_BILL
//	token( encode = '110')
//	COLS(BUID,DEALTOTAL,RDATE,BILLINGDATE)
//	KEYCOLS(BUID);
//	生成extfile内容
	public static String generateExtContent(ConfUtil cu,String extname){
		StringBuffer extContent =new StringBuffer(EXTHEAD.replaceAll("#EXTNAME#", extname));
		List<String> schemas= cu.getSchemas();
		List<Table> tables= cu.getTables();
		
		for(String schema:schemas){
			for(Table table:tables){
				extContent.append(EXTMODEL.replaceAll("#SCHEMA#", schema).replaceAll("#TABLE#", table.getTable_name())
				.replaceAll("#COLS#", table.getColumnstr()).replaceAll("#KEYCOLS#", table.getKeyCol()));
			}
		}
		return extContent.toString();
	}

	
//	生成EXT参数文件
	public static String generateExtFILE(ConfUtil cu,String extname,String content){
		String extfilename=cu.getVal("WORKDIR")+extname+Config.PARFILESUFFIX;
		logger.debug("extfilename="+extfilename);
		File f = new File(extfilename);
		BufferedWriter fw =null;
//		if (!f.getParentFile().exists()) {
//			f.getParentFile().mkdirs();
//		}
		
		try {
			if (!f.exists()) {
				f.createNewFile();
			}
			fw =new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "GBK")) ;
			fw.write(content);
			fw.flush();
			
		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return extfilename;
	}
	

}
