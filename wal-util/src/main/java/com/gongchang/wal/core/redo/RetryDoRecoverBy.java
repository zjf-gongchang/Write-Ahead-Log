package com.gongchang.wal.core.redo;

/**
 * 定义通过何种方式恢复RetryDo
 */
public enum RetryDoRecoverBy {

    /**
     * 通过Spring容器恢复
     */
    SPRING,

    /**
     * 通过反射恢复
     */
    REFLECT,

    /**
     * 通过反序列化
     */
    DESERIALIZATION;

}
