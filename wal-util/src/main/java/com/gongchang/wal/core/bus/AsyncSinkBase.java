package com.gongchang.wal.core.bus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.Barrier;
import com.gongchang.wal.core.base.StreamData;
import com.gongchang.wal.core.base.WalConstant;
import com.gongchang.wal.core.base.WalEntry;

public abstract class AsyncSinkBase implements AsyncSink {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkBase.class);


    private final ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();

    private CyclicBarrier consumeCyclicBarrier = new CyclicBarrier(2);
    
    private AtomicReference<Barrier> currBarrie = new AtomicReference<>(new Barrier());
    
    private AtomicBoolean consumeWaiting = new AtomicBoolean(true);
    
    private ExecutorService sinkExecutorService;
    
    private String walParentPathName;


    protected AsyncSinkBase(SinkConfig sinkConfig) {
    	this.sinkExecutorService = sinkConfig.getSinkExecutorService();
    	this.scheduleExecutorService.scheduleWithFixedDelay(() -> {
            // 广播发送检查点信息
    		Barrier preBarrie = currBarrie.get();
    		if(currBarrie.compareAndSet(preBarrie, new Barrier())){
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
    	// 数据合法性校验，校验不通过则返回false
    	
    	// 数据预提交
    	walEntry.setBarrieId(currBarrie.get().getBarrieId());
    	Boolean preCommit = preCommit(walEntry);
    	// 唤醒Sink消费线程
    	if(preCommit && consumeWaiting.compareAndSet(true, false)){
    		try {
				consumeCyclicBarrier.await(1, TimeUnit.NANOSECONDS);
			} catch (Exception e) {
			} 
    	}
    	// 返回处理结果
    	return preCommit;
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
    
    public abstract Boolean broadcast(Barrier barrier);
    

    private void submitToSinkPool(){
    	Iterator<StreamData> iterator = consume();
    	
    	while(true){
    		if(consumeWaiting.compareAndSet(false, true)){
    			try {
    				consumeCyclicBarrier.await();
    			} catch (Exception e) {
    			} 
    		}
    		
    		while(iterator.hasNext()){
    			StreamData sd = iterator.next();
    			switch (sd.getStreamDataType()) {
    			case BARRIE:
    				Barrier barrier = (Barrier)sd;
    				commit(barrier.getBarrieId());
    				break;
    			case BUSINESS:
    				sinkExecutorService.submit(new AsyncSinkThread((WalEntry)sd));
    				break;
    			default:
    				throw new RuntimeException("无法解析的流数据类型");
    			}
    		}
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
