package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.List;


public class LogUtil {

	private BufferedWriter fw;
	public static String WARN="WARN ";
	public static String ERROR="ERROR ";
	public static String INFO="INFO ";
	
	public static String SCHEMADIV="~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~";
	public static String SQLDIV   ="---------------------------------";

	public LogUtil(String logfile) {
		File f = new File(logfile);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		if (f.exists()) {
			f.delete();
		}
		try {
			f.createNewFile();
//			fw = new BufferedWriter(new OutputStreamWriter(
//					new FileOutputStream(f), ConfigInfo.getValue("encoding")));
			fw = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(f), "UTF8"));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void logMsg(String msg) {
		try {
			fw.write(msg + System.getProperty("line.separator")); // 换行符 \n
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void logMsg(List<String> msglist) {
		for (String msg : msglist) {
			try {

				fw.write(msg + System.getProperty("line.separator")); // 换行符 \n

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			fw.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	  public static String getStackTrace(Throwable aThrowable) {
	        final Writer result = new StringWriter();
	        final PrintWriter printWriter = new PrintWriter(result);
	        aThrowable.printStackTrace(printWriter);
	        return result.toString();
	      }
}
