package com.gongchang.wal.core.write;

import java.util.function.Supplier;

import com.gongchang.wal.core.clean.AbstractWriteAheadLogCutClean;
import com.gongchang.wal.core.clean.WriteAheadLogForCount;
import com.gongchang.wal.core.clean.WriteAheadLogForSize;
import com.gongchang.wal.core.clean.WriteAheadLogForTime;
import com.gongchang.wal.core.clean.WriteAheadLogForTime.TimeUnit;

public class WriteStrInstance {
	
	
	WriteAheadLog<String> writeAheadLog;
	
	
	private WriteStrInstance(WriteAheadLog<String> writeAheadLog){
		this.writeAheadLog = writeAheadLog;
	}
	
	
	public WriteAheadLog<String> getWriteAheadLog() {
		return writeAheadLog;
	}

	public WriteInstanceBuilder getWriteInstanceBuilder(String logName){
		return new WriteInstanceBuilder(logName);
	}
	
	
	private static class WriteInstanceBuilder{
		
		private String logName;
		
		private Integer maxHisLogNum = -1;
		
		private Boolean isSync = true;
		
		private Supplier<AbstractWriteAheadLogCutClean> abstractWalccGetSupplier;

		
		public WriteInstanceBuilder(String logName) {
			super();
			this.logName = logName;
		}

		public WriteStrInstance build(){
			AbstractWriteAheadLogCutClean abstractwalcc = abstractWalccGetSupplier.get();
			if(maxHisLogNum!=-1){
				abstractwalcc.setMaxHisLogNum(maxHisLogNum);
			}
			
			WriteAheadLog<String> writeAheadLog;
			if(isSync){
				writeAheadLog = new SyncWriteAheadLog(logName, abstractwalcc);
			}else{
				writeAheadLog = new AsyncWriteAheadLog(logName, abstractwalcc);
			}
			
			return new WriteStrInstance(writeAheadLog);
		}
		

		public void setMaxHisLogNum(Integer maxHisLogNum) {
			this.maxHisLogNum = maxHisLogNum;
		}

		public void enableLogCutForSize(Long maxLogSize) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForSize(logName, maxLogSize);
				}
			};
		}

		public void enableLogCutForCount(Long maxLogCount) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForCount(logName, maxLogCount);
				}
			};
		}

		public void enableLogCutForTime(TimeUnit maxTimeUnit) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForTime(logName, maxTimeUnit);
				}
			};
		}

		public void enableASync() {
			this.isSync = false;
		}
		
	}
	
}
