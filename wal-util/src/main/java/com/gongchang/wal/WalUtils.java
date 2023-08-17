package com.gongchang.wal;

import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.sink.AsyncSinkManager;
import com.gongchang.wal.core.sink.SinkConfig;

public class WalUtils {
	
	
	private static final AsyncSinkManager asyncSinkManager = AsyncSinkManager.getInstance();
	
	
	public static void recover(){
		
	}
	
	public static void recover(String logName){
		
	}
	
	public static void write(WalEntry walEntry){
//		asyncSinkManager.getDefaultAsyncSink().preCommit(walEntry);
	}
	
	public static void write(SinkConfig sinkConfig, WalEntry walEntry){
//		asyncSinkManager.getAsyncSinkByConfig(sinkConfig).preCommit(walEntry);
	}

	/*private static class RetryDoRecoverExecuteBySpring extends AbstractRetryDoRecover {

        ConfigurableApplicationContext applicationContext;


        public RetryDoRecoverExecuteBySpring(ConfigurableApplicationContext applicationContext) {
            this.applicationContext = applicationContext;
        }

        @Override
        public RetryDo reflect(Class<?> clazz) {
            RetryDo retryDo = (RetryDo)applicationContext.getBean(clazz);
            return retryDo;
        }
    }*/
}
