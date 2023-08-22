package com.gongchang.wal.core.bus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import com.gongchang.wal.core.base.WalConstant;
import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.read.ReadInstance;
import com.gongchang.wal.core.redo.AbstractRetryDoRecover;
import com.gongchang.wal.core.redo.RetryDo;
import com.gongchang.wal.core.redo.RetryDoRecover;
import com.gongchang.wal.core.redo.RetryDoRecoverBy;
import com.gongchang.wal.exception.WalRecoverException;

public class RecoverContext {
	
	private static final Logger logger = LoggerFactory.getLogger(RecoverContext.class);
			

	private Map<RetryDoRecoverBy, RetryDoRecover> retryDoRecoverMap = new HashMap<>();

	private static final RecoverContext recoverContext = new RecoverContext();

	static {
        recoverContext.registerRetryDoRecover(RetryDoRecoverBy.REFLECT, new AbstractRetryDoRecover(){
            @Override
            public RetryDo reflect(Class<?> clazz) {
                try {
                    return (RetryDo)clazz.newInstance();
                } catch (InstantiationException e) {
                    logger.error("通过反射获取实例异常", e);
                    throw new WalRecoverException("通过反射获取实例异常", e);
                } catch (IllegalAccessException e) {
                    logger.error("通过反射获取实例异常", e);
                    throw new WalRecoverException("通过反射获取实例异常", e);
                }
            }
        });
    }
	
    public static RecoverContext getInstance(){
        return recoverContext;
    }
    

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
    
    
    /**
     * 当使用Spring容器获取恢复数据的执行器时候，AbstractRetryDoRecover的实现方式如下：
     * 
	    private static class RetryDoRecoverExecuteBySpring extends AbstractRetryDoRecover {
	
	        ConfigurableApplicationContext applicationContext;
	
	        public RetryDoRecoverExecuteBySpring(ConfigurableApplicationContext applicationContext) {
	            this.applicationContext = applicationContext;
	        }
	
	        @Override
	        public RetryDo reflect(Class<?> clazz) {
	            RetryDo retryDo = (RetryDo)applicationContext.getBean(clazz);
	            return retryDo;
	        }
	    }
	 * 
	 * 调用方式如下（下面第一行代码是SpringBoot程序的启动代码）：
	 * ConfigurableApplicationContext applicationContext = SpringApplication.run(Application.class, args);
	 * RecoverContext.getInstance().enableSpringContextRecover(new RetryDoRecoverExecuteBySpring(applicationContext));
	 * 
     * @param retryDoRecover
     */
    public void enableSpringContextRecover(AbstractRetryDoRecover retryDoRecover){
        registerRetryDoRecover(RetryDoRecoverBy.SPRING, retryDoRecover);
    }
    
    
    public Boolean recover(RecoverConfig recoverConfig){
        boolean result = true;
        try {
            List<DataRecoverThread> dataRecoverThreadList = recoverConfig.getReadInstanceList()
            		.stream()
            		.map(ri -> new DataRecoverThread(ri))
            		.collect(Collectors.toList());

            ExecutorService executorService = Executors.newFixedThreadPool(dataRecoverThreadList.size()>3 ? 3 : dataRecoverThreadList.size());

            result = executorService.invokeAll(dataRecoverThreadList).stream()
                    .allMatch(booleanFuture -> {
                        try {
                            return booleanFuture.get();
                        } catch (InterruptedException e) {
                            logger.error("获取数据恢复结果中断异常：", e);
                            throw new WalRecoverException("获取数据恢复结果中断异常", e);
                        } catch (ExecutionException e) {
                            logger.error("获取数据恢复结果执行异常：", e);
                            throw new WalRecoverException("获取数据恢复结果执行异常", e);
                        }
                    });

            executorService.shutdown();
            if(!executorService.isTerminated()){
                logger.info("等待数据恢复线程执行完成");
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                logger.info("数据恢复线程执行完成，线程池已关闭");
            }
        } catch (InterruptedException e) {
            logger.error("数据恢复中断：", e);
            throw new WalRecoverException("数据恢复中断", e);
        }

        return result;
    }
    
    
    private static class DataRecoverThread implements Callable<Boolean>{

        private ReadInstance readInstance;
        
        private Long barrieId;

        public DataRecoverThread(ReadInstance readInstance) {
                this.readInstance = readInstance;
                Path checkPointPath = Paths.get(System.getProperty("user.dir"), WalConstant.WAL_ROOT_CATALOG, readInstance.getBusinessName(), "checkpoint.txt");
                try {
                	if(Files.exists(checkPointPath)){
                		byte[] readAllBytes = Files.readAllBytes(checkPointPath);
                		barrieId = Long.parseLong(new String(readAllBytes));
                	}else{
                		barrieId = Long.MIN_VALUE;
                	}
				} catch (IOException e) {
					logger.error("读取检查点信息异常", e);
					throw new WalRecoverException("读取检查点信息异常", e);
				}
        }

        @Override
        public Boolean call() throws Exception {
            Iterator<WalEntry> iterator = readInstance.read();
            Boolean result = true;
            while (iterator.hasNext()){
            	WalEntry walEntry = iterator.next();
            	if(walEntry.getBarrieId()>barrieId){
            		Boolean redoResult = walEntry.getRetryDo().redo(walEntry);
            		if(!redoResult){
            			result = false;
            			break;
            		}
            	}
            }
            return result;
        }

    }

}
