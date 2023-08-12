package com.gongchang.wal.core.base;

/**
 * 预写日志配置类
 */
public class WalConfig {

    private WalConfig(){}

    public static final Long CHECKPOINT_MAX_LOG_COUNT = 10000L;

    public static final Long CHECKPOINT_MAX_LOG_SIZE = 100*1024*1024L;

}
