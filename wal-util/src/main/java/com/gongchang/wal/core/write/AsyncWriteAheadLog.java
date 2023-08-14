package com.gongchang.wal.core.write;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.exception.WALWriteException;

public abstract class AsyncWriteAheadLog extends AbstractWriteAheadLog<String> {

	private static final Logger logger = LoggerFactory.getLogger(AsyncWriteAheadLog.class);
	
	
	private static IOException ioException;
	
	private AtomicBoolean schedule = new AtomicBoolean(true);
	
	private final LinkedBlockingQueue<String> cacheQueue = new LinkedBlockingQueue<>(WalConfig.ASYNC_WRITE_LOG_QUEUE_SIZE);
	
	private final ScheduledExecutorService writeLogExecutor = Executors.newSingleThreadScheduledExecutor();
	
	
	public AsyncWriteAheadLog(String logName) {
		super(logName);
	}

	public AsyncWriteAheadLog(String logName, Integer maxHisLogNum) {
		super(logName, maxHisLogNum);
	}
	

	@Override
	public void writeLog(String value) throws IOException {
		if(ioException!=null){
			throw ioException;
		}
		
		cacheQueue.add(value);
		
		if(schedule.compareAndSet(true, false)){
			writeLogExecutor.scheduleWithFixedDelay(new WriteLogThread(this), 1, 1, TimeUnit.SECONDS);
			logger.info("异步写日志线程启动");
		}
	}
	
	private void writeLog() throws IOException{
		try(BufferedWriter bw = Files.newBufferedWriter(getCurWalPath(), StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
        	int count = 0;
        	while(cacheQueue.peek()!=null && count<WalConfig.ASYNC_WRITE_LOG_BATCH_SIZE){
				String value = cacheQueue.poll();
				if(value!=null){
					bw.write(value);
					bw.newLine();
					checkCutLog(value.getBytes().length);
				}else{
					break;
				}
			}
            bw.flush();
        }catch (IOException e) {
            logger.error("写入预写日志异常", e);
            throw e;
        }
	}
	
	private int getCacheQueueSize(){
		return cacheQueue.size();
	}
	
	private static class WriteLogThread implements Runnable{

		private AsyncWriteAheadLog asyncWriteAheadLog;
		
		
		public WriteLogThread( AsyncWriteAheadLog asyncWriteAheadLog) {
			super();
			this.asyncWriteAheadLog = asyncWriteAheadLog;
		}


		@Override
		public void run() {
			try {
				while(asyncWriteAheadLog.getCacheQueueSize()>WalConfig.ASYNC_WRITE_LOG_BATCH_SIZE/2){
					asyncWriteAheadLog.writeLog();
				}
			} catch (IOException e) {
				logger.error("批量写日志异常", e);
				ioException = e;
				throw new WALWriteException("批量写日志异常", e);
			}
		}
		
	}

}
