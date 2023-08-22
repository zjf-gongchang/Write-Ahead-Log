package com.gongchang.wal.core.bus;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.Barrie;
import com.gongchang.wal.core.base.StreamData;
import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.write.WriteInstance;

public class AsyncSinkInMemory extends AsyncSinkBase {
	
	private static final Logger logger = LoggerFactory.getLogger(AsyncSinkInMemory.class);
	
	private LinkedBlockingDeque<StreamData> walEntryQueue = new LinkedBlockingDeque<>(WalConfig.ASYNC_WRITE_LOG_QUEUE_SIZE);
	
	private WriteInstance writeInstance;
	
	private SinkConfig sinkConfig;

	
	public AsyncSinkInMemory(){
		this(SinkConfig.getSinkConfigBuilder("wal").build());
	}
	
	public AsyncSinkInMemory(SinkConfig sinkConfig) {
		super(sinkConfig);
		this.sinkConfig = sinkConfig;
		this.writeInstance = sinkConfig.getWriteInstance();
	}


	@Override
	public Boolean preCommit(WalEntry walEntry) {
		// 写预写日志
        try {
        	writeInstance.writeLog(walEntry);
        } catch (IOException e) {
            return false;
        }
        // 添加到内存队列
        Boolean addResult = walEntryQueue.add(walEntry);
		return addResult;
	}
	
	public SinkConfig getSinkConfig() {
		return sinkConfig;
	}

	@Override
	public Iterator<StreamData> consume() {
		return new Iterator<StreamData>() {
			
			@Override
			public StreamData next() {
				return walEntryQueue.poll();
			}
			
			@Override
			public boolean hasNext() {
				return walEntryQueue.peek()!=null;
			}
		};
	}

	@Override
	public Boolean broadcast(Barrie barrie) {
		return walEntryQueue.add(barrie);
	}

}
