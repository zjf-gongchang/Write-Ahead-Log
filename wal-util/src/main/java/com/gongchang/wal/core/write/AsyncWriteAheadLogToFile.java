package com.gongchang.wal.core.write;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.core.clean.WriteAheadLogCutClean;
import com.gongchang.wal.exception.WALWriteException;

public class AsyncWriteAheadLogToFile extends AbstractWriteAheadLogToFile<String> {

	private static final Logger logger = LoggerFactory.getLogger(AsyncWriteAheadLogToFile.class);
	
	
	private final LinkedBlockingQueue<String> cacheQueue = new LinkedBlockingQueue<>(WalConfig.ASYNC_WRITE_LOG_QUEUE_SIZE);
	
	private final ScheduledExecutorService writeLogExecutor = Executors.newSingleThreadScheduledExecutor();
	
	private static IOException writeException;
	
	private AtomicBoolean schedule = new AtomicBoolean(true);
	
	private WriteAheadLogCutClean walcc;
	
	
	public AsyncWriteAheadLogToFile(String logName) {
		super(logName);
	}

	public AsyncWriteAheadLogToFile(String logName, WriteAheadLogCutClean walcc) {
		super(logName);
		this.walcc = walcc;
	}
	

	@Override
	public void writeLog(String value) throws IOException {
		if(writeException!=null){
			throw writeException;
		}
		
		cacheQueue.add(value);
		
		if(schedule.compareAndSet(true, false)){
			writeLogExecutor.scheduleWithFixedDelay(new WriteLogThread(this), 1, 1, TimeUnit.SECONDS);
			logger.info("异步写日志线程启动");
		}
	}
	
	private void batchWriteLog() throws IOException{
		Path walCurPath = PathUtils.getWalCurPath(getLogName());
		try(BufferedWriter bw = Files.newBufferedWriter(walCurPath, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
        	int count = 0;
        	while(cacheQueue.peek()!=null && count<WalConfig.ASYNC_WRITE_LOG_BATCH_SIZE){
				String value = cacheQueue.poll();
				if(value!=null){
					bw.write(value);
					bw.newLine();
					
					cutLog(value);
				}else{
					break;
				}
			}
            bw.flush();
        }catch (IOException e) {
            logger.error("写入预写日志异常", e);
            throw new WALWriteException("写入预写日志异常", e);
        }
	}
	
	private void cutLog(String value){
		 if(walcc!=null){
        	try {
				walcc.cutLog(value.getBytes().length);
			} catch (IOException e) {
				logger.error("日志切割异常", e);
			}
        }
	}
	
	private int getCacheQueueSize(){
		return cacheQueue.size();
	}
	
	private static class WriteLogThread implements Runnable{

		private AsyncWriteAheadLogToFile asyncWriteAheadLogToFile;
		
		
		public WriteLogThread( AsyncWriteAheadLogToFile asyncWriteAheadLogToFile) {
			super();
			this.asyncWriteAheadLogToFile = asyncWriteAheadLogToFile;
		}


		@Override
		public void run() {
			try {
				while(asyncWriteAheadLogToFile.getCacheQueueSize()>WalConfig.ASYNC_WRITE_LOG_BATCH_SIZE/2){
					asyncWriteAheadLogToFile.batchWriteLog();
				}
			} catch (IOException e) {
				logger.error("批量写日志异常", e);
				writeException = e;
			}
		}
		
	}

}
