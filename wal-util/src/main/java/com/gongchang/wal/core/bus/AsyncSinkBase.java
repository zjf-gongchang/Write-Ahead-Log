package com.gongchang.wal.core.bus;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.core.base.WalEntry;

public abstract class AsyncSinkBase implements AsyncSink {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkBase.class);


    private final ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();

    private ExecutorService sinkExecutorService;
    
    

    private Long  checkPointId  = 0L;

    private AtomicLong logCount = new AtomicLong(0);

    private AtomicLong logSize = new AtomicLong(0);
    
    private AtomicBoolean committing = new AtomicBoolean(false);


    protected AsyncSinkBase(SinkConfig sinkConfig) {
    	this.sinkExecutorService = sinkConfig.getSinkExecutorService();
    	this.scheduleExecutorService.scheduleWithFixedDelay(() -> {
            if(submitToSinkPool()){
                commit(checkPointId);
            }
        }, sinkConfig.getCheckPointInterval(), sinkConfig.getCheckPointInterval(), TimeUnit.SECONDS);
    }

    public Boolean sink(WalEntry walEntry){
    	// 数据合法性校验，校验不通过则返回false
    	if(!walEntry.getWalDataCheck().check(walEntry.getData())){
    		return false;
    	}

    	// 数据预提交
    	Boolean preCommit = preCommit(walEntry);
    	
    	// 唤醒Sink拉取线程
    	
    	
        // 检查提交检查点
        checkCommit(walEntry.getData().toJSONString().getBytes().length);

        return preCommit;
    }
    
    private void checkCommit(int byteSize){
        if(logCount.incrementAndGet()>=WalConfig.CHECKPOINT_MAX_LOG_COUNT || logSize.addAndGet(byteSize)>=WalConfig.CHECKPOINT_MAX_LOG_SIZE){
        	if(committing.compareAndSet(false, true)){
        		scheduleExecutorService.submit(() -> {
        			commit(checkPointId);
        			committing.getAndSet(false);
        		});
            }
        }
    }
    
    public abstract Boolean preCommit(WalEntry walEntry);

    public abstract Iterator<WalEntry> iterator();
    
    public abstract Boolean commit(Long checkPointId);
    
    

    private Boolean submitToSinkPool(){
        List<Future<Boolean>> futures = new ArrayList<>();
        Iterator<WalEntry> iterator = iterator();
        while(iterator.hasNext()){
            WalEntry walEntry = iterator.next();
            Future<Boolean> future = sinkExecutorService.submit(new AsyncSinkThread(walEntry));
            futures.add(future);
            Long createTime = walEntry.getCreateTime();
            if(createTime<checkPointId){
                checkPointId = createTime;
            }
        }

        for (Future<Boolean> future: futures) {
            try {
                if(!future.get()){
                    return false;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return true;
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
