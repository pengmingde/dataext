package main;


import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import util.ConfUtil;
import util.ValidateUtil;



public class Dataext {
	static Logger logger = Logger.getLogger(Dataext.class.getName());
	public static void main(String[] args){
	String curPath = args[0];
	if (curPath != null && !curPath.endsWith("/")) {
		curPath = curPath + "/";
	}
	PropertyConfigurator.configure(curPath + "log4j.prop");
	logger.debug("curPath:" + curPath);

	ConfUtil.CONFFILE=curPath+"conf.prop";
	ConfUtil configUtil = ConfUtil.getInstance();
//	logger.debug(configUtil);
	ValidateUtil.validate(configUtil);
	}
}
