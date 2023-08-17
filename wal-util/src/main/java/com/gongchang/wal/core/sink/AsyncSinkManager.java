package com.gongchang.wal.core.sink;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步下沉管理器
 */
public class AsyncSinkManager {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkManager.class);

    
    private static final Map<SinkConfig, AsyncSink<?,?>> asyncSinkMap = new HashMap<SinkConfig, AsyncSink<?,?>>();

    
    private SinkConfig defaultSinkConfig;
    
    private AsyncSinkManager(){}

    private static class AsyncSinkManagerGet{
    	private static final AsyncSinkManager asyncSinkManager = new AsyncSinkManager();
    }
    
    public static AsyncSinkManager getInstance(){
    	return AsyncSinkManagerGet.asyncSinkManager;
    }
    

    public synchronized AsyncSink<?,?> getDefaultAsyncSink(){
    	if(defaultSinkConfig==null){
    		AsyncSinkBase asyncSinkBase = new AsyncSinkBase();
    		asyncSinkMap.put(asyncSinkBase.getSinkConfig(), asyncSinkBase);
    		setDefaultSinkConfig(asyncSinkBase.getSinkConfig());
    	}
        return asyncSinkMap.get(defaultSinkConfig);
    }
    
    public synchronized AsyncSink<?,?> getAsyncSinkByConfig(SinkConfig sinkConfig){
    	AsyncSink<?, ?> asyncSink = asyncSinkMap.get(defaultSinkConfig);
    	if(asyncSink==null){
    		AsyncSinkBase asyncSinkBase = new AsyncSinkBase(sinkConfig);
    		asyncSinkMap.put(asyncSinkBase.getSinkConfig(), asyncSinkBase);
    		return asyncSinkBase;
    	}else{
    		return asyncSink;
    	}
    }


	public SinkConfig getDefaultSinkConfig() {
		return defaultSinkConfig;
	}

	private void setDefaultSinkConfig(SinkConfig defaultSinkConfig) {
		this.defaultSinkConfig = defaultSinkConfig;
	}

}
