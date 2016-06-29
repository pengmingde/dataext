package util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;


public class ConnUtil {
	static Logger logger = Logger.getLogger(ConnUtil.class.getName());
	public static void freeRs(ResultSet rs){
		if(rs!=null)
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(LogUtil.getStackTrace(e));
			}
	}
	
	public static void freeStat(Statement st){
		if(st!=null)
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error(LogUtil.getStackTrace(e));
			}
	}
}
