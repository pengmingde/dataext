package main;


import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import config.Config;
import util.ConfUtil;
import util.FileUtil;
import util.ParamFileUtil;
import util.SystemUtil;
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
	ConfUtil cu = ConfUtil.getInstance();
//	logger.debug(configUtil);
	ValidateUtil.validate(cu);
	String extname=cu.getVal("EXTNAME");
//	生成extract文件并传输到源端服务器目录
	String parContent=ParamFileUtil.generateExtContent(cu,extname);
	String extParFile= ParamFileUtil.generateExtFILE(cu, extname, parContent);
	String remoteDir=FileUtil.getOggDir("SOURCEOGGHOME", Config.DIRPRM)+new File(extParFile).getName();
	logger.debug("remoteDir="+remoteDir);
		SystemUtil.whenErrorThenExit(
				FileUtil.scpFileToSource(extParFile, remoteDir),
				"File transfer " + extParFile + " fail,exit!", "File transfer "
						+ extParFile + " success!");
		logger.debug("test return");	
		
//	生成源端defpar文件并传输到源端目录
		String defParContent=ParamFileUtil.generateSourceDefParContent(cu,extname);
		String defParFile= ParamFileUtil.generateSourceDefParFILE(cu, extname, defParContent);
		remoteDir=FileUtil.getOggDir("SOURCEOGGHOME", Config.DIRPRM)+new File(defParFile).getName();
		logger.debug("remoteDir="+remoteDir);
		SystemUtil.whenErrorThenExit(
				FileUtil.scpFileToSource(defParFile, remoteDir),
				"File transfer " + defParFile + " fail,exit!", "File transfer "
						+ defParFile + " success!");
//	logger.debug(ParamFileUtil.generateExtContent(cu,extname));
	}
}
