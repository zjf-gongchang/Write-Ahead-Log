package com.gongchang.wal.core.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

/**
 * 异步下沉工厂类
 */
public class AsyncSinkFactory {

    private static final Logger logger = LoggerFactory.getLogger(AsyncSinkFactory.class);


    private AsyncSinkFactory(){}


    public static AsyncSink getDefaultAsyncSink(){
        return DefaultAsyncSinkGet.defaultAsyncSink;
    }

    public static AsyncSink getCustomAsyncSink(){
        throw new RuntimeException("该功能尚未实现");
    }

    private static class DefaultAsyncSinkGet{
        private static final AsyncSink defaultAsyncSink = DefaultParamAsyncSink.getAsyncSinkBuilder().build();
    }


    public static void main(String[] args) {
        AsyncSink defaultAsyncSink = AsyncSinkFactory.getDefaultAsyncSink();

        JSONObject data = new JSONObject();
        data.put("Title", "心情不好，今天下大雨了，但是庆幸的是明天不会下大雨，大大大大大雨");
        data.put("SiteName", "微博");
        data.put("Url", "http://sina.com/importApi/single");
        data.put("PublishTime", "2023-07-28T12:02:55.686Z");
        data.put("Sentiment", "正");
        data.put("Carrie", "2001");

        Long start = System.currentTimeMillis();
        /*YqDataAnalysisSingle yqDataAnalysisSingle = new YqDataAnalysisSingle();
        for(int i=0; i<10; i++){
            defaultAsyncSink.preCommit(new WalEntry(yqDataAnalysisSingle, data));
        }*/
        Long end = System.currentTimeMillis();
        System.out.println("耗时（ms）："+(end-start));
    }

}
