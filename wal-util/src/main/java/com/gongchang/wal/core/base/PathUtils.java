package com.gongchang.wal.core.base;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class PathUtils {
	
	private static final Path WAL_ROOT_PATH = Paths.get(System.getProperty("user.dir"), WalConstant.WAL_ROOT_CATALOG);
	
	
	private PathUtils(){}
	
	
	public static final Path getWalRootPath(){
		return WAL_ROOT_PATH;
	}
	
	public static final Path getWalParentPath(String walFileName){
		return Paths.get(WAL_ROOT_PATH.toString(), walFileName);
	}
	
	public static final Path getWalCurPath(String walFileName){
		return Paths.get(WAL_ROOT_PATH.toString(), walFileName, walFileName+".log");
	}
	
	public static final Path getWalRollPath(String walFileName, String tarLogPatternName){
		return Paths.get(
				WAL_ROOT_PATH.toString(), 
				walFileName, 
				new StringBuilder().append(walFileName).append("-").append(tarLogPatternName).append(".log").toString());
	}
	
	public static final Path getBakLogPath(String walFileName) {
		return Paths.get(
				WAL_ROOT_PATH.toString(), 
				walFileName, 
				new StringBuilder().append(walFileName).append("-").append(System.currentTimeMillis()).append("-bak.log").toString());
	}
	
}
