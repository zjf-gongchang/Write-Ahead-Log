package com.gongchang.wal.core.sink;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultParamAsyncSink extends AsyncSinkBase {

    private static final Logger logger = LoggerFactory.getLogger(DefaultParamAsyncSink.class);


    private DefaultParamAsyncSink() throws IOException {
        super();
    }

    static AsyncSinkBuilder getAsyncSinkBuilder(){
        return new AsyncSinkBuilder();
    }

    public static class AsyncSinkBuilder{
        public AsyncSink build(){
            try {
                return new DefaultParamAsyncSink();
            } catch (IOException e) {
                logger.error("构建默认异步器异常：", e);
            }
            return null;
        }
    }

}
