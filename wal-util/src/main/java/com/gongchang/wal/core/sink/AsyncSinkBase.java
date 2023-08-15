package com.gongchang.wal.core.sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.WalConfig;
import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.write.SyncWriteAheadLog;
import com.gongchang.wal.core.write.WriteAheadLog;

public class AsyncSinkBase implements AsyncSink<Long, WalEntry> {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkBase.class);


    private WriteAheadLog rwal;

    private final ScheduledExecutorService scheduleExecutorService = Executors.newSingleThreadScheduledExecutor();

    private LinkedBlockingDeque<WalEntry> walEntryQueue;

    private ExecutorService sinkExecutorService;

    private Long  checkPointId  = 0L;

    private AtomicLong logCount = new AtomicLong(0);

    private AtomicLong logSize = new AtomicLong(0);


    protected AsyncSinkBase() throws IOException {
        rwal = new SyncWriteAheadLog("rwal");
        sinkExecutorService = new ThreadPoolExecutor(
                1,
                10,
                3000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(5000));
        walEntryQueue = new LinkedBlockingDeque<>(10000);
        scheduleExecutorService.scheduleWithFixedDelay(() -> {
            if(submitToSinkPool()){
                commit(checkPointId);
            }
        },10*60, 10*60, TimeUnit.SECONDS);
    }

    protected AsyncSinkBase(String logName, Integer sinkInitThreadNum, Integer sinkMaxThreadNum, Integer sinkQueueSize, Integer walQueueSize, Long checkPointInterval) throws IOException {
        rwal = new SyncWriteAheadLog(logName);
        sinkExecutorService = new ThreadPoolExecutor(
                sinkInitThreadNum,
                sinkMaxThreadNum,
                30*1000L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(sinkQueueSize));
        walEntryQueue = new LinkedBlockingDeque<>(walQueueSize);
        scheduleExecutorService.scheduleWithFixedDelay(() -> {
            if(submitToSinkPool()){
                commit(checkPointId);
            }
        },checkPointInterval, checkPointInterval, TimeUnit.SECONDS);
    }


    @Override
    public Boolean preCommit(WalEntry walEntry) {
        // 数据合法性校验，校验不通过则返回false

        // 写预写日志
        try {
            rwal.writeLog(walEntry.metaToMementoStr());
        } catch (IOException e) {
            return false;
        }

        // 添加到队列
        Boolean addResult = walEntryQueue.add(walEntry);

        // 检查提交检查点
        checkCommit(walEntry.getData().toJSONString().getBytes().length);

        return addResult;
    }

    @Override
    public Boolean commit(Long checkPointId) {
        // 记录检查点信息
        Path checkPointPath = Paths.get(System.getProperty("user.dir"), "wal", "checkpoint.txt");
        try {
            Files.write(checkPointPath, String.valueOf(checkPointId).getBytes(),StandardOpenOption.CREATE);
        } catch (IOException e) {
            logger.error("持久化检查点异常：", e);
            return false;
        }
        return true;
    }

    private void checkCommit(int byteSize){
        if(logCount.incrementAndGet()>=WalConfig.CHECKPOINT_MAX_LOG_COUNT || logSize.addAndGet(byteSize)>=WalConfig.CHECKPOINT_MAX_LOG_SIZE){
            scheduleExecutorService.submit(() -> {
                if(submitToSinkPool()){
                    commit(checkPointId);
                }
            });
        }
    }

    private Boolean submitToSinkPool(){
        List<Future<Boolean>> futures = new ArrayList<>();
        while(walEntryQueue.peek()!=null){
            WalEntry walEntry = walEntryQueue.poll();
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


    public static void main(String[] args) {
        Path checkPointPath = Paths.get(System.getProperty("user.dir"), "checkpoint.txt");
        try {
            Files.write(checkPointPath, String.valueOf("hkkk").getBytes(),StandardOpenOption.CREATE);
        } catch (IOException e) {
            logger.error("持久化检查点异常：", e);
        }
    }

}
