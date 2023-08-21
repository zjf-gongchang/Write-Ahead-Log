package com.gongchang.wal.core.bus;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步下沉管理器
 */
public class AsyncSinkManager {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkManager.class);

    
    private static final Map<SinkConfig, AsyncSink> asyncSinkMap = new HashMap<SinkConfig, AsyncSink>();

    
    private SinkConfig defaultSinkConfig;
    
    private AsyncSinkManager(){}

    private static class AsyncSinkManagerGet{
    	private static final AsyncSinkManager asyncSinkManager = new AsyncSinkManager();
    }
    
    public static AsyncSinkManager getInstance(){
    	return AsyncSinkManagerGet.asyncSinkManager;
    }
    

    public AsyncSink getDefaultAsyncSink(){
    	if(defaultSinkConfig==null){
    		synchronized (this) {
    			if(defaultSinkConfig==null){
    				AsyncSinkInMemory asyncSinkInMemory = new AsyncSinkInMemory();
    	    		asyncSinkMap.put(asyncSinkInMemory.getSinkConfig(), asyncSinkInMemory);
    	    		setDefaultSinkConfig(asyncSinkInMemory.getSinkConfig());
    			}
			}
    	}
        return asyncSinkMap.get(defaultSinkConfig);
    }
    
    public AsyncSink getAsyncSinkByConfig(SinkConfig sinkConfig){
    	AsyncSink asyncSink = asyncSinkMap.get(sinkConfig);
    	if(asyncSink==null){
    		synchronized (this) {
    			if(asyncSink==null){
    				switch (sinkConfig.getCacheQuqueType()) {
					case MEMORY:
						asyncSink = new AsyncSinkInMemory(sinkConfig);
						break;
					case KAFKA:
						throw new RuntimeException("该功能尚未实现");
					default:
						throw new RuntimeException("不支持的缓冲队列类型");
					}
    	    		asyncSinkMap.put(sinkConfig, asyncSink);
    			}
			}
    	}
    	return asyncSinkMap.get(sinkConfig);
    }


	public SinkConfig getDefaultSinkConfig() {
		return defaultSinkConfig;
	}

	private void setDefaultSinkConfig(SinkConfig defaultSinkConfig) {
		this.defaultSinkConfig = defaultSinkConfig;
	}

}
