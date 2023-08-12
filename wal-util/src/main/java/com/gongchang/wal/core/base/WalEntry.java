package com.gongchang.wal.core.base;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.gongchang.DataRecoverContext;
import com.gongchang.wal.core.redo.RetryDo;
import com.gongchang.wal.core.redo.RetryDoRecover;


public class WalEntry {

    /**
     * 恢复数据的类的实例对象
     */
    private RetryDo retryDo;

    /**
     * 数据
     */
    private JSONObject data;

    /**
     * 创建时间
     */
    private Long createTime = System.currentTimeMillis();


    public WalEntry(RetryDo retryDo, JSONObject data) {
        this.retryDo = retryDo;
        this.data = data;
    }

    private WalEntry(RetryDo retryDo, JSONObject data, Long createTime) {
        this.retryDo = retryDo;
        this.data = data;
        this.createTime = createTime;
    }

    public String metaToMementoStr() {
        WalEntryMeta walEntryMeta = new WalEntryMeta(
                retryDo.getClass().getName(),
                retryDo.getRecoverBy().name(),
                data,
                createTime);
        return JSON.toJSONString(walEntryMeta);
    }

    public static WalEntryMeta metaFromMementoStr(String metaStr){
        JSONObject metaJsonObj = JSON.parseObject(metaStr);
        WalEntryMeta walEntryMeta = metaJsonObj.toJavaObject(WalEntryMeta.class);
        return walEntryMeta;
    }


    public static class WalEntryMeta{

        private String retryDoClassName;

        private String retryDoRecoverBy;

        private JSONObject dada;

        private Long createTime;


        public WalEntryMeta(String retryDoClassName, String retryDoRecoverBy, JSONObject dada, Long createTime) {
            this.retryDoClassName = retryDoClassName;
            this.retryDoRecoverBy = retryDoRecoverBy;
            this.dada = dada;
            this.createTime = createTime;
        }


        public WalEntry metaToWalEntry(){
            RetryDoRecover retryDoRecover = DataRecoverContext.getRecoverContext().requestRetryDoRecover(retryDoRecoverBy);
            RetryDo retryDo = retryDoRecover.recover(retryDoClassName);
            WalEntry walEntry = new WalEntry(retryDo, dada, createTime);
            return walEntry;
        }


        public String getRetryDoClassName() {
            return retryDoClassName;
        }

        public void setRetryDoClassName(String retryDoClassName) {
            this.retryDoClassName = retryDoClassName;
        }

        public String getRetryDoRecoverBy() {
            return retryDoRecoverBy;
        }

        public void setRetryDoRecoverBy(String retryDoRecoverBy) {
            this.retryDoRecoverBy = retryDoRecoverBy;
        }

        public JSONObject getDada() {
            return dada;
        }

        public void setDada(JSONObject dada) {
            this.dada = dada;
        }

        public Long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(Long createTime) {
            this.createTime = createTime;
        }

    }


    public RetryDo getRetryDo() {
        return retryDo;
    }

    public void setRetryDo(RetryDo retryDo) {
        this.retryDo = retryDo;
    }

    public JSONObject getData() {
        return data;
    }

    public void setData(JSONObject data) {
        this.data = data;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

}
