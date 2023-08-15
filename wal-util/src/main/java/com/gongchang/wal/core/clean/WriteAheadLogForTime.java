package com.gongchang.wal.core.clean;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;

/**
 * 按时间单位切分日志
 */
public class WriteAheadLogForTime extends AbstractWriteAheadLogCutClean {

	private static final Logger logger = LoggerFactory.getLogger(WriteAheadLogForTime.class);
	
	
	public enum TimeUnit{
		DAY
	}
	
	private LocalDateTime curLogMaxDateTime;
	
	private TimeUnit timeUnit;
	
	
    public WriteAheadLogForTime(String walFileName) {
        this(walFileName, TimeUnit.DAY, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }
    
    public WriteAheadLogForTime(String walFileName, TimeUnit timeUnit) {
    	this(walFileName, timeUnit, WalConfig.DEFAULT_MAX_HIS_LOG_NUM);
    }

    public WriteAheadLogForTime(String walFileName, TimeUnit timeUnit, Integer maxHisLogNum) {
		super(walFileName, maxHisLogNum);
		this.timeUnit = timeUnit;
		
		computeCurLogMaxTime();
	}


	@Override
    public Boolean whetherToCut(Integer logSize) {
        return LocalDateTime.now().isAfter(curLogMaxDateTime);
    }
	
	@Override
    public List<String> getCleanLogName() {
        return getDicOrderLogName(getMaxHisLogNum());
    }

    @Override
    public String getLogPatternName() {
    	String logPatternName = curLogMaxDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
    	computeCurLogMaxTime();
        return logPatternName;
    }
    
    private void computeCurLogMaxTime(){
    	switch (timeUnit) {
		case DAY:
			curLogMaxDateTime = LocalDateTime.of(LocalDate.now(), LocalTime.MAX);
			break;
		default:
			throw new IllegalArgumentException("不能识别的日志切割单位："+timeUnit.name());
		}
    }
    
    public static void main(String[] args) {
    	LocalDateTime localDateTime = LocalDateTime.now();
		String format = localDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		System.out.println(format);
	}

}
