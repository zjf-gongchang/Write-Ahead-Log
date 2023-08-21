package com.gongchang.wal;

import com.gongchang.wal.core.bus.RecoverConfig;
import com.gongchang.wal.core.bus.RecoverContext;

/**
 * 预写日志恢复工具
 */
public class WalRecoverUtils {

    
    private static final RecoverContext recoverContext = RecoverContext.getInstance();


    public static Boolean recover(){
    	return recoverContext.recover(RecoverConfig.getRecoverConfigBuilder().build());
    }
    
    public static Boolean recover(RecoverConfig recoverConfig){
    	return recoverContext.recover(recoverConfig);
    }
    
}
