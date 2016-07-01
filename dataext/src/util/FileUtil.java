package util;

import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
//import com.jcraft.jsch.Logger;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class FileUtil {
	static Logger logger = Logger.getLogger(FileUtil.class.getName());
	private static Session sessionSource;
	private static ChannelSftp sftpChannelSource;
	
	private static Session sessionTarget;
	private static ChannelSftp sftpChannelTarget;
	
	private static ConfUtil cu = ConfUtil.getInstance();
	static{
//		String SOURCEOSUSER=cu.getVal("SOURCEOSUSER");
		try {
			sessionSource = new JSch().getSession(cu.getVal("SOURCEOSUSER"), cu.getVal("SOURCEDBIP"), Integer.parseInt(cu.getVal("SOURCESSHPORT")));
	        sessionSource.setConfig("StrictHostKeyChecking", "no");
	        sessionSource.setPassword(cu.getVal("SOURCEOSPASS"));
	        sessionSource.connect(); 
	        Channel channelSource = sessionSource.openChannel("sftp");
	        channelSource.connect();
	        sftpChannelSource = (ChannelSftp) channelSource; 
	        
	        sessionTarget = new JSch().getSession(cu.getVal("TARGETOSUSER"), cu.getVal("TARGETDBIP"), Integer.parseInt(cu.getVal("TARGETSSHPORT")));
	        sessionTarget.setConfig("StrictHostKeyChecking", "no");
	        sessionTarget.setPassword(cu.getVal("TARGETOSPASS"));
	        sessionTarget.connect(); 
	        Channel channelTarget = sessionSource.openChannel("sftp");
	        channelTarget.connect();
	        sftpChannelTarget = (ChannelSftp) channelTarget; 
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static boolean scpFileToSource(String localFile,String remoteDir){
		boolean succ=true;
		try {
			sftpChannelSource.put(localFile,remoteDir);
		} catch (SftpException e) {
			// TODO Auto-generated catch block
			logger.error(LogUtil.getStackTrace(e));
			succ=false;
		}
		return succ;
	}
	@Override
	protected void finalize() throws Throwable {
		// TODO Auto-generated method stub
		super.finalize();
		sftpChannelSource.exit();
        sessionSource.disconnect();
        sftpChannelTarget.exit();
        sessionTarget.disconnect();
	}
	
	
	public static String toDirFormat(String dirname){
		return dirname.endsWith("/")?dirname:dirname+"/";
	}
	
//	 获取ogg的一个子目录 ogghome为TARGETOGGHOME或SOURCEOGGHOME,subdir为Config.DIRPRM .etc
	public static String getOggDir(String ogghome,String subdir){
		String ogghomedir=toDirFormat(cu.getVal(ogghome));
		return ogghomedir+subdir+"/";
	}
//	
//	String hostname = "192.168.1.52";
//    String username = "oracle";
//    String password = "oracle";
//    String copyFrom = "/tmp/aaaaaa";
//    String copyTo = "C:/Users/Administrator/Desktop/tmp/a"; 
//    JSch jsch = new JSch();
//    Session session = null;
//    System.out.println("Trying to connect.....");
//        session = jsch.getSession(username, hostname, 22);
//        session.setConfig("StrictHostKeyChecking", "no");
//        session.setPassword(password);
//        session.connect(); 
//        Channel channel = session.openChannel("sftp");
//        channel.connect();
//        ChannelSftp sftpChannel = (ChannelSftp) channel; 
////        sftpChannel.get(copyFrom, copyTo);
//        sftpChannel.put(copyTo, copyFrom);
}
