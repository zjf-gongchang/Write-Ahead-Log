package com.gongchang.wal;

import com.gongchang.wal.core.base.WalEntry;
import com.gongchang.wal.core.bus.AsyncSinkManager;
import com.gongchang.wal.core.bus.SinkConfig;

public class WalSinkUtils {
	
	
	private static final AsyncSinkManager asyncSinkManager = AsyncSinkManager.getInstance();
	
	
	public static void write(WalEntry walEntry){
		asyncSinkManager.getDefaultAsyncSink().sink(walEntry);
	}
	
	public static void write(SinkConfig sinkConfig, WalEntry walEntry){
		asyncSinkManager.getAsyncSinkByConfig(sinkConfig).sink(walEntry);
	}

}
