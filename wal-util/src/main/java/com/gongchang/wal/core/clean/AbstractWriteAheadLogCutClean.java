package com.gongchang.wal.core.clean;

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

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.core.base.WalConfig;

public abstract class AbstractWriteAheadLogCutClean implements WriteAheadLogCutClean {

	private static final Logger logger = LoggerFactory.getLogger(AbstractWriteAheadLogCutClean.class);
	
	
	private final ScheduledExecutorService logProcessSchedule = Executors.newSingleThreadScheduledExecutor();

	private String walFileName;
	
	private Integer maxHisLogNum;
	
	
	public AbstractWriteAheadLogCutClean(String walFileName) {
		this(walFileName, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
	}
	
	public AbstractWriteAheadLogCutClean(String walFileName, Integer maxHisLogNum) {
		super();
		this.walFileName = walFileName;
		this.maxHisLogNum = maxHisLogNum;
		// 初始化日志定时清理任务
		logProcessSchedule.scheduleWithFixedDelay(() -> cleanLog(PathUtils.getWalParentPath(walFileName)), 60, 60, TimeUnit.MINUTES);
	}
	
	
	@Override
	public void cutLog(Integer logSize) throws IOException {
		if(whetherToCut(logSize)){
            try {
            	cutWriteAheadLog();
			} catch (IOException e) {
				logger.error("检查切割日志异常", e);
				throw e;
			}
        }
	}
	
	@Override
	public Boolean cleanLog(Path logParentPath) {
		try {
			List<String> cleanLogNameList = getCleanLogName();
			for (String cleanLogname : cleanLogNameList) {
				Files.delete(Paths.get(logParentPath.toString(), cleanLogname));
			}
		} catch (IOException e) {
			logger.error("历史日志清理异常：", e);
			return false;
		}
		return true;
	}
	
	public void cutWriteAheadLog() throws IOException {
		try {
			Path walCurPath = PathUtils.getWalCurPath(walFileName);
			Files.move(walCurPath, PathUtils.getWalRollPath(walFileName, getLogPatternName()));
			Files.createFile(walCurPath);
		} catch (IOException e) {
			logger.error("切割预写日志异常", e);
			throw e;
		}		
	}
	
	public abstract Boolean whetherToCut(Integer logSize);
	
	public abstract String getLogPatternName();
	
    public abstract List<String> getCleanLogName();
	
	public List<String> getDicOrderLogName(Integer maxHisLogNum) {
        Path logParentPath = PathUtils.getWalParentPath(walFileName);
        try {
            List<String> waitCleanLogNameList = Files.list(logParentPath).flatMap(path -> {
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

	
	public String getWalFileName() {
		return walFileName;
	}

	public void setWalFileName(String walFileName) {
		this.walFileName = walFileName;
	}

	public Integer getMaxHisLogNum() {
		return maxHisLogNum;
	}

	public void setMaxHisLogNum(Integer maxHisLogNum) {
		this.maxHisLogNum = maxHisLogNum;
	}
	
}
