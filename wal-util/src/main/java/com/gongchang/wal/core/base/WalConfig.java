package com.gongchang.wal.core.base;

/**
 * 预写日志配置类
 */
public class WalConfig {

    private WalConfig(){}

    
    public static final Integer ASYNC_WRITE_LOG_QUEUE_SIZE = 10000;
    
    public static final Integer ASYNC_WRITE_LOG_BATCH_SIZE = 1000;
    
    public static final Long DEFAULT_MAX_LOG_SIZE = 1*1024*1024*1024L;
    
    public static final Long DEFAULT_MAX_LOG_COUNT = 1*1024*1024*1024L;

    public static final Integer DEFAULT_MAX_HIS_LOG_NUM = 7;

}
