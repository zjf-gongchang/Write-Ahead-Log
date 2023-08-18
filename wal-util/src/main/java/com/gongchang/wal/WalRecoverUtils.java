package com.gongchang.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gongchang.wal.core.bus.RecoverConfig;
import com.gongchang.wal.core.bus.RecoverContext;

/**
 * 预写日志恢复工具
 */
public class WalRecoverUtils {

    private static final Logger logger = LoggerFactory.getLogger(WalRecoverUtils.class);
    
    
    private static final RecoverContext recoverContext = RecoverContext.getInstance();


    public static Boolean recover(){
    	return recoverContext.recover();
    }
    
    public static Boolean recover(RecoverConfig recoverConfig){
    	
    	return true;
    }
    
}
