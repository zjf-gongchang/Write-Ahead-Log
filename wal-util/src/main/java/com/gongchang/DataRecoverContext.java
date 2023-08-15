package com.gongchang;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.base.PathUtils;
import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.read.ReadAheadLog;
import com.gongchang.wal.core.read.ReadAheadLogImpl;
import com.gongchang.wal.core.redo.AbstractRetryDoRecover;
import com.gongchang.wal.core.redo.RetryDo;
import com.gongchang.wal.core.redo.RetryDoRecover;
import com.gongchang.wal.core.redo.RetryDoRecoverBy;

/**
 * 数据恢复上下文
 */
public class DataRecoverContext {

    private static final Logger logger = LoggerFactory.getLogger(DataRecoverContext.class);


    private static final RecoverContext recoverContext = new RecoverContext();

    public static RecoverContext getRecoverContext(){
        return recoverContext;
    }

    public static class RecoverContext{

        private Map<RetryDoRecoverBy, RetryDoRecover> retryDoRecoverMap = new HashMap<>();


        private void registerRetryDoRecover(RetryDoRecoverBy retryDoRecoverBy, RetryDoRecover retryDoRecover){
            retryDoRecoverMap.put(retryDoRecoverBy, retryDoRecover);
        }

        public RetryDoRecover requestRetryDoRecover(RetryDoRecoverBy retryDoRecoverBy){
            return retryDoRecoverMap.get(retryDoRecoverBy);
        }

        public RetryDoRecover requestRetryDoRecover(String retryDoRecoverByName){
            RetryDoRecoverBy retryDoRecoverBy = RetryDoRecoverBy.valueOf(retryDoRecoverByName);
            return retryDoRecoverMap.get(retryDoRecoverBy);
        }
    }

    static {
        recoverContext.registerRetryDoRecover(RetryDoRecoverBy.REFLECT, new AbstractRetryDoRecover(){
            @Override
            public RetryDo reflect(Class<?> clazz) {
                try {
                    return (RetryDo)clazz.newInstance();
                } catch (InstantiationException e) {
                    logger.error("通过反射获取实例异常", e);
                    throw new RuntimeException("通过反射获取实例异常", e);
                } catch (IllegalAccessException e) {
                    logger.error("通过反射获取实例异常", e);
                    throw new RuntimeException("通过反射获取实例异常", e);
                }
            }
        });
    }

    public static void enableSpringContextRecover(AbstractRetryDoRecover retryDoRecover){
        recoverContext.registerRetryDoRecover(RetryDoRecoverBy.SPRING, retryDoRecover);
    }

    public static Boolean recoverAll(){
        boolean result = true;
        try {
            List<DataRecoverThread> readAheadLogList = Files
                    .list(PathUtils.getWalRootPath())
                    .map(path -> new DataRecoverThread(new ReadAheadLogImpl(path)))
                    .collect(Collectors.toList());

            ExecutorService executorService = Executors.newFixedThreadPool(readAheadLogList.size()>3 ? 3 : readAheadLogList.size());

            result = executorService.invokeAll(readAheadLogList).stream()
                    .allMatch(booleanFuture -> {
                        try {
                            return booleanFuture.get();
                        } catch (InterruptedException e) {
                            logger.error("获取数据恢复结果中断异常：", e);
                        } catch (ExecutionException e) {
                            logger.error("获取数据恢复结果执行异常：", e);
                        }
                        return false;
                    });

            executorService.shutdown();
            if(!executorService.isTerminated()){
                logger.info("等待数据恢复线程执行完成");
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                logger.info("数据恢复线程执行完成，线程池已关闭");
            }
        } catch (IOException e) {
            logger.error("数据恢复IO异常：", e);
        } catch (InterruptedException e) {
            logger.error("数据恢复中断异常：", e);
        }

        return result;
    }

    private static class DataRecoverThread implements Callable<Boolean>{

        private ReadAheadLog readAheadLog;

        public DataRecoverThread(ReadAheadLog readAheadLog) {
                this.readAheadLog = readAheadLog;
            }

            @Override
            public Boolean call() throws Exception {
                Iterator<String> iterator = readAheadLog.readLog();
                Boolean result = true;
                while (iterator.hasNext()){
                    String metaMemtroStr = iterator.next();

                    WalEntry walEntry = WalEntry.metaFromMementoStr(metaMemtroStr).metaToWalEntry();

                    Boolean redoResult = walEntry.getRetryDo().redo(walEntry);
                    if(!redoResult){
                        result = false;
                        break;
                    }
                }
                return result;
        }

    }

}
