package com.gongchang.wal.core.write;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.core.base.WalConstant;
import com.gongchang.wal.exception.WALInitCheckException;

public abstract class AbstractWriteAheadLog<T> implements WriteAheadLog<T> {

	private static final Logger logger = LoggerFactory.getLogger(AbstractWriteAheadLog.class);

	
	private final ScheduledExecutorService logProcessSchedule = Executors.newSingleThreadScheduledExecutor();

	private String logName;
	
	private Integer maxHisLogNum;
	
	private Path walParentPath = null;
	
	Path curWalPath = null;

	
	public AbstractWriteAheadLog(String logName) {
		this(logName, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
	}
	
	public AbstractWriteAheadLog(String logName, Integer maxHisLogNum) {
		super();
		this.logName = logName;
		this.maxHisLogNum = maxHisLogNum;
		
		try {
			init();
		} catch (IOException e) {
			logger.error("预写日志路径未能正常初始化", e);
			throw new WALInitCheckException("预写日志未能正常初始化", e);
		}
	}


	private void init() throws IOException {
		// 初始化日志定时清理任务
		logProcessSchedule.scheduleWithFixedDelay(() -> cleanLog(), 60, 60, TimeUnit.MINUTES);

		// 初始化预写日志根路径
		if (!Files.exists(WalConstant.WAL_ROOT_PATH)) {
			try {
				Files.createDirectory(WalConstant.WAL_ROOT_PATH);
			} catch (IOException e) {
				logger.error("创建预写日志根目录异常，路径信息：{}", WalConstant.WAL_ROOT_PATH.toString());
				throw e;
			}
		}

		// 初始化预写日志路径
		Path walParentPath = createWalParentPath();
		if (!Files.exists(walParentPath)) {
			try {
				Files.createDirectory(walParentPath);
			} catch (IOException e) {
				logger.error("创建预写日志父目录异常，路径信息：{}", walParentPath.toString());
				throw e;
			}
		}

		// 初始化当前预写日志
		Path curWalPath = createCurWalPath();
		if (!Files.exists(curWalPath)) {
			try {
				Files.createFile(curWalPath);
			} catch (IOException e) {
				logger.error("创建预写日志文件异常，路径信息：{}", curWalPath.toString());
				throw e;
			}
		} else {
			Path tarWalPath = getNextLogPath();
			try {
				if (Files.size(curWalPath) > 0) {
					moveCurLogToHisAndCreate(curWalPath, tarWalPath);
				}
			} catch (IOException e) {
				logger.error("移动预写日志异常，源路径信息：{}，，目标路径信息：{}", curWalPath.toString(), tarWalPath.toString());
				throw e;
			}
		}
	}
	
	public Boolean cleanLog() {
		try {
			List<String> cleanLogNameList = getCleanLogName();
			for (String cleanLogname : cleanLogNameList) {
				Files.delete(Paths.get(WalConstant.WAL_ROOT_PATH.toString(), cleanLogname));
			}
		} catch (IOException e) {
			logger.error("历史日志清理异常：", e);
			return false;
		}
		return true;
	}

	protected Path createWalParentPath() {
		walParentPath = Paths.get(WalConstant.WAL_ROOT_PATH.toString(), this.logName);
		return walParentPath;
	}
	
	protected Path createCurWalPath() {
		curWalPath = Paths.get(walParentPath.toString(), this.logName + ".log");
		return curWalPath;
	}

	protected Path getNextLogPath() {
		return Paths.get(walParentPath.toString(), logName + "-" + getNextLogName() + ".log");
	}

	protected void moveCurLogToHisAndCreate(Path curPath, Path tarPath) throws IOException {
		try {
			Files.move(curPath, tarPath);
			Files.createFile(curPath);
		} catch (IOException e) {
			logger.error("移动当前日志异常", e);
			throw e;
		}
	}
	
	protected void checkCutLog(Integer logSize) throws IOException {
		if(whetherToCut(logSize)){
            try {
				moveCurLogToHisAndCreate(curWalPath, getNextLogPath());
			} catch (IOException e) {
				logger.error("检查切割日志异常", e);
				throw e;
			}
        }
	}
	
	public List<String> getDicOrderCleanLogName() {
        Path walParentPath = getWalParentPath();
        try {
            List<String> waitCleanLogNameList = Files.list(walParentPath).flatMap(path -> {
                List<String> tmpList = new ArrayList<>();
                String fileName = path.getFileName().toString();
                if (fileName.indexOf("-") > 0) {
                    tmpList.add(fileName);
                }
                return tmpList.stream();
            }).sorted().collect(Collectors.toList());

            List<String> cleanLogNameList;
            if(waitCleanLogNameList.size()<maxHisLogNum){
                cleanLogNameList=waitCleanLogNameList;
            }else{
                cleanLogNameList = new ArrayList<>();
                for(int i=0; i<waitCleanLogNameList.size()-maxHisLogNum; i++){
                    cleanLogNameList.add(waitCleanLogNameList.get(i));
                }
            }

            return  cleanLogNameList;
        } catch (IOException e) {
            logger.error("获取需要清理的日志列表异常：", e);
        }
        return Collections.emptyList();
    }
	
	protected abstract String getNextLogName();

	protected abstract List<String> getCleanLogName();
	
	public abstract Boolean whetherToCut(Integer logSize);
	
	public Path getWalParentPath() {
		return walParentPath;
	}
	
	public Path getCurWalPath() {
		return curWalPath;
	}

}
