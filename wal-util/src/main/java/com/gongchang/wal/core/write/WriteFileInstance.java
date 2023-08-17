package com.gongchang.wal.core.write;

import java.io.IOException;
import java.util.function.Supplier;

import com.gongchang.wal.core.clean.AbstractWriteAheadLogCutClean;
import com.gongchang.wal.core.clean.WriteAheadLogForCount;
import com.gongchang.wal.core.clean.WriteAheadLogForSize;
import com.gongchang.wal.core.clean.WriteAheadLogForTime;
import com.gongchang.wal.core.clean.WriteAheadLogForTime.TimeUnit;

public class WriteFileInstance implements WriteInstance<String> {
	
	
	WriteAheadLog<String> writeAheadLog;
	
	
	private WriteFileInstance(WriteAheadLog<String> writeAheadLog){
		this.writeAheadLog = writeAheadLog;
	}
	
	
	public WriteAheadLog<String> getWriteAheadLog() {
		return writeAheadLog;
	}

	public static WriteInstanceBuilder getWriteInstanceBuilder(String logName){
		return new WriteInstanceBuilder(logName);
	}
	
	
	public static class WriteInstanceBuilder{
		
		private String logName;
		
		private Integer maxHisLogNum = -1;
		
		private Boolean isSync = true;
		
		private Supplier<AbstractWriteAheadLogCutClean> abstractWalccGetSupplier;

		
		public WriteInstanceBuilder(String logName) {
			super();
			this.logName = logName;
		}

		public WriteFileInstance build(){
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
			
			return new WriteFileInstance(writeAheadLog);
		}
		

		public WriteInstanceBuilder setMaxHisLogNum(Integer maxHisLogNum) {
			this.maxHisLogNum = maxHisLogNum;
			return this;
		}

		public WriteInstanceBuilder enableLogCutForSize(Long maxLogSize) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForSize(logName, maxLogSize);
				}
			};
			return this;
		}

		public WriteInstanceBuilder enableLogCutForCount(Long maxLogCount) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForCount(logName, maxLogCount);
				}
			};
			return this;
		}

		public WriteInstanceBuilder enableLogCutForTime(TimeUnit maxTimeUnit) {
			abstractWalccGetSupplier = new Supplier<AbstractWriteAheadLogCutClean>() {
				@Override
				public AbstractWriteAheadLogCutClean get() {
					return new WriteAheadLogForTime(logName, maxTimeUnit);
				}
			};
			return this;
		}

		public WriteInstanceBuilder enableASync() {
			this.isSync = false;
			return this;
		}
		
	}


	@Override
	public void writeLog(String value) throws IOException {
		writeAheadLog.writeLog(value);
	}
	
}
