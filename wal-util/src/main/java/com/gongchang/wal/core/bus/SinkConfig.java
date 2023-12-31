package com.gongchang.wal.core.bus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.write.WriteFileInstance;
import com.gongchang.wal.core.write.WriteInstance;

public class SinkConfig {
	
	private String businessName;
	
	private CacheQuqueType cacheQuqueType;

	private WriteInstance writeInstance;
	
	private ExecutorService sinkExecutorService;

    private LinkedBlockingDeque<WalEntry> walEntryQueue;
    
    private Long checkPointInterval;
    
	
	public SinkConfig(String businessName, CacheQuqueType cacheQuqueType, WriteInstance writeInstance, ExecutorService sinkExecutorService,
			LinkedBlockingDeque<WalEntry> walEntryQueue, Long checkPointInterval) {
		super();
		this.businessName = businessName;
		this.writeInstance = writeInstance;
		this.sinkExecutorService = sinkExecutorService;
		this.walEntryQueue = walEntryQueue;
		this.checkPointInterval = checkPointInterval;
	}

	
	public static SinkConfigBuilder getSinkConfigBuilder(String businessName){
		return new SinkConfigBuilder(businessName);
	}
	
	
	public static class SinkConfigBuilder{
		
		private CacheQuqueType cacheQuqueType = CacheQuqueType.MEMORY;
		
		private String businessName;
		
		private WriteInstance writeInstance;
		
		private Integer sinkPoolInitThreadNum = 1;
		
		private Integer sinkPoolMaxThreadNum = 10;
		
		private Long sinkPoolKeepAliveTime = 3000L;
		
		private Integer sinkPoolQueueSize = 5000;
		
		private Integer walQueueSize = 10000;
		
		private Long checkPointInterval = 10*60L;


		public SinkConfigBuilder(String businessName) {
			super();
			this.businessName = businessName;
			writeInstance = WriteFileInstance.getWriteInstanceBuilder(businessName).enableLogCutForSize(Long.valueOf(1*1024*1024*1024)).build();
		}
		
		
		public SinkConfig build(){
			ThreadPoolExecutor sinkExecutorService = new ThreadPoolExecutor(
					sinkPoolInitThreadNum,
					sinkPoolMaxThreadNum,
					sinkPoolKeepAliveTime,
	                TimeUnit.MILLISECONDS,
	                new LinkedBlockingQueue<>(sinkPoolQueueSize));
			LinkedBlockingDeque<WalEntry> walEntryQueue = new LinkedBlockingDeque<>(walQueueSize);
			return new SinkConfig(businessName, cacheQuqueType, writeInstance, sinkExecutorService, walEntryQueue, checkPointInterval);
		}
		
		
		public void setWriteInstance(WriteInstance writeInstance) {
			this.writeInstance = writeInstance;
		}

		public void setCacheQuqueType(CacheQuqueType cacheQuqueType) {
			this.cacheQuqueType = cacheQuqueType;
		}

		public SinkConfigBuilder setSinkPoolInitThreadNum(Integer sinkPoolInitThreadNum) {
			this.sinkPoolInitThreadNum = sinkPoolInitThreadNum;
			return this;
		}

		public SinkConfigBuilder setSinkPoolMaxThreadNum(Integer sinkPoolMaxThreadNum) {
			this.sinkPoolMaxThreadNum = sinkPoolMaxThreadNum;
			return this;
		}

		public SinkConfigBuilder setSinkPoolKeepAliveTime(Long sinkPoolKeepAliveTime) {
			this.sinkPoolKeepAliveTime = sinkPoolKeepAliveTime;
			return this;
		}

		public SinkConfigBuilder setSinkPoolQueueSize(Integer sinkPoolQueueSize) {
			this.sinkPoolQueueSize = sinkPoolQueueSize;
			return this;
		}

		public SinkConfigBuilder setWalQueueSize(Integer walQueueSize) {
			this.walQueueSize = walQueueSize;
			return this;
		}

		public SinkConfigBuilder setCheckPointInterval(Long checkPointInterval) {
			this.checkPointInterval = checkPointInterval;
			return this;
		}
		
	}
	

	public String getBusinessName() {
		return businessName;
	}

	public CacheQuqueType getCacheQuqueType() {
		return cacheQuqueType;
	}

	public WriteInstance getWriteInstance() {
		return writeInstance;
	}

	public ExecutorService getSinkExecutorService() {
		return sinkExecutorService;
	}

	public LinkedBlockingDeque<WalEntry> getWalEntryQueue() {
		return walEntryQueue;
	}

	public Long getCheckPointInterval() {
		return checkPointInterval;
	}
	
}
