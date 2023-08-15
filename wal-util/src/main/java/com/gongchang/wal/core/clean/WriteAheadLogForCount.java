package com.gongchang.wal.core.clean;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

/**
 * 按统计条数切分日志
 */
public class WriteAheadLogForCount extends AbstractWriteAheadLogCutClean {

	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForCount.class);
	
	
	private AtomicInteger logCount = new AtomicInteger(0);
	
	private Long maxLogCount;

    
    public WriteAheadLogForCount(String walFileName) throws IOException {
        this(walFileName, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }
    
    public WriteAheadLogForCount(String walFileName, Integer maxHisLogNum) {
		super(walFileName, maxHisLogNum);
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
    public String getLogPatternName() {
    	return String.valueOf(System.currentTimeMillis());
    }

}
