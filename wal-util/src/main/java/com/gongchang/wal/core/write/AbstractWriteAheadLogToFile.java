package com.gongchang.wal.core.write;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.exception.WALInitCheckException;

public abstract class AbstractWriteAheadLogToFile<T> implements WriteAheadLog<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractWriteAheadLogToFile.class);

	
	private String logName;
	
	
	public AbstractWriteAheadLogToFile(String logName) {
		super();
		this.logName = logName;
		
		try {
			init();
		} catch (IOException e) {
			logger.error("预写日志路径未能正常初始化", e);
			throw new WALInitCheckException("预写日志未能正常初始化", e);
		}
	}


	private void init() throws IOException {
		// 初始化预写日志根路径
		Path walRootPath = PathUtils.getWalRootPath();
		if (!Files.exists(walRootPath)) {
			try {
				Files.createDirectory(walRootPath);
			} catch (IOException e) {
				logger.error("创建预写日志根目录异常，路径信息：{}", walRootPath.toString());
				throw e;
			}
		}

		// 初始化预写日志路径
		Path walParentPath = PathUtils.getWalParentPath(logName);
		if (!Files.exists(walParentPath)) {
			try {
				Files.createDirectory(walParentPath);
			} catch (IOException e) {
				logger.error("创建预写日志父目录异常，路径信息：{}", walParentPath.toString());
				throw e;
			}
		}

		// 初始化当前预写日志
		Path curWalPath = PathUtils.getWalCurPath(logName);
		if (!Files.exists(curWalPath)) {
			try {
				Files.createFile(curWalPath);
			} catch (IOException e) {
				logger.error("创建预写日志文件异常，路径信息：{}", curWalPath.toString());
				throw e;
			}
		} else {
			try {
				if (Files.size(curWalPath) > 0) {
					bakWriteAheadLog();
				}
			} catch (IOException e) {
				logger.error("系统初始化移动预写日志异常，源路径信息：{}", curWalPath.toString());
				throw e;
			}
		}
	}
	
	private void bakWriteAheadLog() throws IOException {
		try {
			Path walCurPath = PathUtils.getWalCurPath(logName);
			Files.move(walCurPath, PathUtils.getBakLogPath(logName));
			Files.createFile(walCurPath);
		} catch (IOException e) {
			logger.error("备份历史日志异常", e);
			throw e;
		}
	}
	

	public String getLogName() {
		return logName;
	}

	public void setLogName(String logName) {
		this.logName = logName;
	}

}
