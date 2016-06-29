package util;

public class StringUtil {
//将字符串转换为sql中的in列表的格式，for example: a,b,c to 'a','b','c'
	public static String strToInlist(String str){
		String strArray[] = str.split(",");
		String strsContByComma="";
		for(String sch:strArray){
			strsContByComma+=",'"+sch.toUpperCase().replace("ZT_", "")+"'";
		}
		strsContByComma=strsContByComma.substring(1);
		return strsContByComma;
	}
	
}
