package com.gongchang.wal.core.redo;

import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractRetryDoRecover implements RetryDoRecover {

    private static final Logger logger = LoggerFactory.getLogger(AbstractRetryDoRecover.class);


    private static final Function<String, Class<?>> reflectFunction = className -> {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            logger.error("没有找到数据恢复类：", e);
        }
        return  clazz;
    };

    @Override
    public RetryDo recover(String retryDoClassName) {
        Class<?> clazz = reflectFunction.apply(retryDoClassName);
        RetryDo retryDo = reflect(clazz);
        return retryDo;
    }

    public abstract RetryDo reflect(Class<?> clazz);

}
