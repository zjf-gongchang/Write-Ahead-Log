package com.gongchang.wal.core.clean;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

/**
 * 按天切分日志
 */
public class WriteAheadLogForDay extends AbstractWriteAheadLogCutClean {

	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForDay.class);
	
	
	private LocalDateTime curDayMaxDateTime = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
	
	
    public WriteAheadLogForDay(String walFileName) throws IOException {
        this(walFileName, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }

    public WriteAheadLogForDay(String walFileName, Integer maxHisLogNum) {
		super(walFileName, maxHisLogNum);
	}


	@Override
    public Boolean whetherToCut(Integer logSize) {
    	LocalDateTime localDateTime = LocalDateTime.now();
    	boolean after = localDateTime.isAfter(curDayMaxDateTime);
    	curDayMaxDateTime = localDateTime;
        return after;
    }

    @Override
    public String getLogPatternName() {
        return curDayMaxDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
    
    
    public static void main(String[] args) {
    	LocalDateTime localDateTime = LocalDateTime.now();
		String format = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		System.out.println(format);
	}

}
