package com.gongchang.wal.core.clean;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

public class WriteAheadLogForSize extends AbstractWriteAheadLogCutClean {

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForSize.class);


    private AtomicLong logSize = new AtomicLong(0);

    private Long maxLogSize;


    public WriteAheadLogForSize(String walFileName) throws IOException {
        this(walFileName, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }

    public WriteAheadLogForSize(String walFileName, Integer maxHisLogNum) throws IOException {
        super(walFileName, maxHisLogNum);
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
    public String getLogPatternName() {
        return String.valueOf(System.currentTimeMillis());
    }

}
