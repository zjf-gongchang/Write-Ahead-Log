package com.gongchang.wal.core.write;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

/**
 * 按统计条数切分日志
 */
public class WriteAheadLogForCount extends SyncWriteAheadLog {

	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForCount.class);
	
	
	private AtomicInteger logCount = new AtomicInteger(0);
	
	private Long maxLogCount;

    
    public WriteAheadLogForCount(String logName) throws IOException {
        this(logName, WalConfig.DEFAULT_MAX_LOG_COUNT);
    }
    
    public WriteAheadLogForCount(String logName, Long maxLogCount) {
		super(logName);
		this.maxLogCount = maxLogCount;
	}
    
	public WriteAheadLogForCount(String logName, Integer maxHisLogNum, Long maxLogCount) {
		super(logName, maxHisLogNum);
		this.maxLogCount = maxLogCount;
	}


	@Override
    public Boolean whetherToCut(Integer logSize) {
        if(this.logCount.incrementAndGet()>=maxLogCount){
            logger.info("当前日志达到最大日志大小，切割日志");
            return true;
        }else{
            return false;
        }
    }

    @Override
    public List<String> getCleanLogName() {
    	return getDicOrderCleanLogName();
    }

    @Override
    public String getNextLogName() {
    	return String.valueOf(System.currentTimeMillis());
    }
    
}
