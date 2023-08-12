package com.gongchang.wal.core.sink;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomParamAsyncSink extends AsyncSinkBase {

    private static final Logger logger = LoggerFactory.getLogger(CustomParamAsyncSink.class);


    private CustomParamAsyncSink(
            String logName,
            Integer sinkInitThreadNum,
            Integer sinkMaxThreadNum,
            Integer sinkQueueSize,
            Integer walQueueSize,
            Long checkPointInterval) throws IOException {

        super(logName, sinkInitThreadNum, sinkMaxThreadNum, sinkQueueSize, walQueueSize, checkPointInterval);
    }

    static AsyncSinkBuilder getAsyncSinkBuilder(){
        return new AsyncSinkBuilder();
    }

    private static class AsyncSinkBuilder{

        String logName = "custom";

        Integer sinkInitThreadNum = 1;

        Integer sinkMaxThreadNum = 10;

        Integer sinkQueueSize = 5000;

        Integer walQueueSize = 10000;

        Long checkPointInterval = 10*60L;


        public void setLogName(String logName) {
            this.logName = logName;
        }

        public void setSinkInitThreadNum(Integer sinkInitThreadNum) {
            this.sinkInitThreadNum = sinkInitThreadNum;
        }

        public void setSinkMaxThreadNum(Integer sinkMaxThreadNum) {
            this.sinkMaxThreadNum = sinkMaxThreadNum;
        }

        public void setSinkQueueSize(Integer sinkQueueSize) {
            this.sinkQueueSize = sinkQueueSize;
        }

        public void setWalQueueSize(Integer walQueueSize) {
            this.walQueueSize = walQueueSize;
        }

        public void setCheckPointInterval(Long checkPointInterval) {
            this.checkPointInterval = checkPointInterval;
        }


        public AsyncSink build(){
            try {
                return new CustomParamAsyncSink(
                        logName,
                        sinkInitThreadNum,
                        sinkMaxThreadNum,
                        sinkQueueSize,
                        walQueueSize,
                        checkPointInterval
                );
            } catch (IOException e) {
                logger.error("构建自定义异步下沉器异常", e);
            }
            return null;
        }

    }

}
