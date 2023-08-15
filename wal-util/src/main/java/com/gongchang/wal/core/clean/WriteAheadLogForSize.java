package com.gongchang.wal.core.clean;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

public class WriteAheadLogForSize extends AbstractWriteAheadLogCutClean {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForSize.class);


    private AtomicLong logSize = new AtomicLong(0);

    private Long maxLogSize;


    public WriteAheadLogForSize(String walFileName) {
        this(walFileName, WalConfig.DEFAULT_MAX_LOG_SIZE, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }
    
    public WriteAheadLogForSize(String walFileName, Long maxLogSize) {
        this(walFileName, maxLogSize, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }

    public WriteAheadLogForSize(String walFileName, Long maxLogSize, Integer maxHisLogNum) {
        super(walFileName, maxHisLogNum);
        this.maxLogSize = maxLogSize;
    }

    
    @Override
    public Boolean whetherToCut(Integer logSize) {
        if(this.logSize.addAndGet(logSize)>=maxLogSize){
            logger.info("当前日志达到最大日志大小，切割日志");
            return true;
        }else{
            return false;
        }
    }
    
    @Override
    public List<String> getCleanLogName() {
        return getDicOrderLogName(getMaxHisLogNum());
    }

    @Override
    public String getLogPatternName() {
        return String.valueOf(System.currentTimeMillis());
    }

    
	public Long getMaxLogSize() {
		return maxLogSize;
	}

	public void setMaxLogSize(Long maxLogSize) {
		this.maxLogSize = maxLogSize;
	}

}
