package util;

import main.Dataext;

import org.apache.log4j.Logger;

public class SystemUtil {
	static Logger logger = Logger.getLogger(SystemUtil.class.getName());
	public static void whenErrorThenExit(boolean bool,String errMsg,String succMsg){
		if(!bool){
			logger.error(errMsg);
			System.exit(-1);
		}else{
			if(succMsg!=null&&!"".equals(succMsg))
				logger.info(succMsg);
		}
	}
}
