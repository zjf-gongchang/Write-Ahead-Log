package com.gongchang.wal.core.bus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.Barrie;
import com.gongchang.wal.core.base.StreamData;
import com.gongchang.wal.core.base.WalConstant;
import com.gongchang.wal.core.base.WalEntry;

public abstract class AsyncSinkBase implements AsyncSink {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkBase.class);


    private final ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();

    private final ReentrantLock consumeLock = new ReentrantLock();
    
    private final Condition consumeCondition = consumeLock.newCondition();
    
    private AtomicReference<Barrie> currBarrie = new AtomicReference<>(new Barrie());
    
    private ExecutorService sinkExecutorService;
    
    private String walParentPathName;


    protected AsyncSinkBase(SinkConfig sinkConfig) {
    	this.sinkExecutorService = sinkConfig.getSinkExecutorService();
    	this.scheduleExecutorService.scheduleWithFixedDelay(() -> {
            // 广播发送检查点信息
    		Barrie preBarrie = currBarrie.get();
    		if(currBarrie.compareAndSet(preBarrie, new Barrie())){
    			broadcast(preBarrie);
    		}
        }, 0, sinkConfig.getCheckPointInterval(), TimeUnit.SECONDS);
    	scheduleExecutorService.submit(new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				submitToSinkPool();
				return true;
			}
		});
    	this.walParentPathName = sinkConfig.getBusinessName();
    }

    public Boolean sink(WalEntry walEntry){
    	try {
    		// 数据合法性校验，校验不通过则返回false
    		
    		// 数据预提交
//    		consumeLock.lock();
    		walEntry.setBarrieId(currBarrie.get().getBarrieId());
    		Boolean preCommit = preCommit(walEntry);
    		// 唤醒Sink消费线程
    		if(preCommit){
//    			consumeCondition.notifyAll();
    		}
    		// 返回处理结果
            return preCommit;
		} finally {
//			consumeLock.unlock();
		}
    }
    
    public Boolean commit(Long checkPointId) {
        // 记录检查点信息
        Path checkPointPath = Paths.get(System.getProperty("user.dir"), WalConstant.WAL_ROOT_CATALOG, walParentPathName, "checkpoint.txt");
        try {
            Files.write(checkPointPath, String.valueOf(checkPointId).getBytes(),StandardOpenOption.CREATE);
        } catch (IOException e) {
            logger.error("持久化检查点异常：", e);
            return false;
        }
        return true;
    }
    
    
    public abstract Boolean preCommit(WalEntry walEntry);

    public abstract Iterator<StreamData> consume();
    
    public abstract Boolean broadcast(Barrie barrie);
    

    private void submitToSinkPool(){
    	try {
//    		consumeLock.lock();
    		
    		Iterator<StreamData> iterator = consume();
    		
    		while(true){
    			while(iterator.hasNext()){
    				StreamData sd = iterator.next();
    				switch (sd.getStreamDataType()) {
    				case BARRIE:
    					Barrie barrie = (Barrie)sd;
    					commit(barrie.getBarrieId());
    					break;
    				case BUSINESS:
    					sinkExecutorService.submit(new AsyncSinkThread((WalEntry)sd));
    					break;
    				default:
    					throw new RuntimeException("无法解析的流数据类型");
    				}
    			}
    			
    			/*try {
    				System.out.println("============================");
	    			consumeCondition.await();
	    			System.out.println("---------------------------");
	    		} catch (InterruptedException e) {
	    			logger.error("消费线程等待中断", e);
	    		}*/
    		}
		} finally {
//			consumeLock.unlock();
		}
    }

    private class AsyncSinkThread implements Callable<Boolean>{

        WalEntry walEntry;

        public AsyncSinkThread(WalEntry walEntry) {
            this.walEntry = walEntry;
        }

        @Override
        public Boolean call() throws Exception {
            Boolean redo = walEntry.getRetryDo().redo(walEntry);
            return redo;
        }
    }

}
