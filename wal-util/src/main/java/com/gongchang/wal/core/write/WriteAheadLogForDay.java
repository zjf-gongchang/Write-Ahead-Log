package com.gongchang.wal.core.write;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 按天切分日志
 */
public class WriteAheadLogForDay extends SyncWriteAheadLog {

	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForDay.class);
	
	
	private LocalDateTime curDayMaxDateTime = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
	
	
    public WriteAheadLogForDay(String logName) throws IOException {
        super(logName);
    }
    

    public WriteAheadLogForDay(String logName, Integer maxHisLogNum) {
		super(logName, maxHisLogNum);
	}


	@Override
    public Boolean whetherToCut(Integer logSize) {
    	LocalDateTime localDateTime = LocalDateTime.now();
    	boolean after = localDateTime.isAfter(curDayMaxDateTime);
    	curDayMaxDateTime = localDateTime;
        return after;
    }

    @Override
    public List<String> getCleanLogName() {
    	return getDicOrderCleanLogName();
    }

    @Override
    public String getNextLogName() {
        return curDayMaxDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    }
    
    
    public static void main(String[] args) {
    	LocalDateTime localDateTime = LocalDateTime.now();
		String format = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		System.out.println(format);
	}
    
}
